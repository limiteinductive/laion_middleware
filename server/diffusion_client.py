import heapq
import random
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

import cv2
import hivemind
import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from hivemind import PeerID, RemoteExpert, TimedStorage
from hivemind.compression import serialize_torch_tensor
from hivemind.dht import DHT
from hivemind.moe.client.expert import DUMMY, expert_forward
from hivemind.moe.client.remote_expert_worker import RemoteExpertWorker
from hivemind.moe.expert_uid import ExpertInfo, ExpertPrefix, ExpertUID
from hivemind.utils import (DHTExpiration, ValueWithExpiration, get_dht_time,
                            get_logger, nested_compare, nested_flatten,
                            nested_pack, use_hivemind_log_handler)
from hivemind.utils.performance_ema import PerformanceEMA
from torch.autograd.function import once_differentiable

logger = get_logger(__name__)


MAX_PROMPT_LENGTH = 512
MAX_NODES = 99999


class LoadBalancer:
    def __init__(
        self,
        dht: DHT,
        key: ExpertPrefix,
        update_period: float = 30.0,
        initial_throughput: float = 1.0,
        **kwargs,
    ):
        self.dht, self.key = dht, key
        self.initial_throughput, self.ema_kwargs = initial_throughput, kwargs
        self.experts = TimedStorage[ExpertUID, PeerID]()
        self.blacklist = TimedStorage[ExpertUID, type(None)]()
        self.throughputs: Dict[ExpertUID, PerformanceEMA] = {}
        self.queue: List[Tuple[float, float, ExpertUID]] = []
        self.uid_to_queue: Dict[ExpertUID, Tuple[float, float, ExpertUID]] = {}
        self.lock = threading.Lock()
        self.is_alive = threading.Event()
        self.is_alive.set()
        self.update_trigger, self.update_finished = threading.Event(), threading.Event()
        self.update_period, self.last_update = update_period, get_dht_time()
        self.update_thread = threading.Thread(
            target=self.update_experts_in_background, daemon=True
        )
        self.update_thread.start()
        self._p2p = RemoteExpertWorker.run_coroutine(self.dht.replicate_p2p())

    def update_experts_in_background(self):
        while self.is_alive.is_set():
            time_to_next_update = max(
                0.0, self.last_update + self.update_period - get_dht_time()
            )
            try:
                self.update_trigger.wait(timeout=time_to_next_update)
                # update triggered by main thread
            except TimeoutError:
                pass  # update triggered by refresh_period

            self.update_trigger.clear()
            response = self.dht.get(self.key, latest=True)
            if isinstance(response, ValueWithExpiration) and isinstance(
                response.value, dict
            ):
                for index, expert_info in response.value.items():
                    try:
                        (expert_uid, peer_id), expiration_time = expert_info

                        maybe_banned = self.blacklist.get(expert_uid)
                        if (
                            maybe_banned is None
                            or expiration_time > maybe_banned.expiration_time
                        ):
                            self._add_expert(expert_uid, peer_id, expiration_time)
                        else:
                            logger.debug(
                                f"Not adding expert {expert_uid} (blacklisted)."
                            )
                    except Exception as e:
                        logger.warning(
                            f"Skipping malformed expert info {expert_info} (exc={e})"
                        )
            else:
                logger.warning(
                    f"Could not refresh experts, dht info key contains {response}, "
                    f"will retry in {time_to_next_update}s"
                )
            if len(self.queue) == 0:
                logger.warning(
                    "Update routine finished, but still no experts available."
                )

            self.last_update = get_dht_time()
            self.update_finished.set()

    def _trigger_updating_experts(self):
        self.update_finished.clear()
        self.update_trigger.set()
        self.update_finished.wait()

    @property
    def n_active_experts(self) -> int:
        if len(self.uid_to_queue) == 0:
            # Maybe it did not do the first update yet
            self._trigger_updating_experts()

        return len(self.uid_to_queue)

    def _add_expert(
        self, uid: ExpertUID, peer_id: PeerID, expiration_time: DHTExpiration
    ):
        with self.lock:
            self.experts.store(uid, peer_id, expiration_time)
            if uid not in self.uid_to_queue:
                logger.debug(
                    f"Adding new expert: {uid}, expiration time = {expiration_time:.3f}."
                )
                self.throughputs[uid] = PerformanceEMA(*self.ema_kwargs, paused=True)
                base_load = self.queue[0][0] if len(self.queue) > 0 else 0.0
                heap_entry = (base_load, random.random(), uid)
                heapq.heappush(self.queue, heap_entry)
                self.uid_to_queue[uid] = heap_entry
            else:
                logger.debug(
                    f"Refreshing existing module: {uid}, new expiration time = {expiration_time:.3f}."
                )

    def _ban_expert(self, uid: ExpertUID):
        with self.lock:
            maybe_expert = self.experts.get(uid)
            expiration_time = (
                maybe_expert.expiration_time if maybe_expert else get_dht_time()
            )
            self.blacklist.store(uid, None, expiration_time)
            self.uid_to_queue.pop(uid, None)
            self.throughputs.pop(uid, None)
            del self.experts[uid]
            logger.debug(
                f"Banned expert {uid} with expiration time = {expiration_time:.2f}."
            )

    @contextmanager
    def use_another_expert(self, task_size: float, max_tries: int = 3) -> RemoteExpert:
        n_tries = 0
        while True:
            if len(self.queue) == 0:
                if n_tries == max_tries:
                    raise NoModulesFound("No modules found in the network")

                n_tries += 1
                self._trigger_updating_experts()
                continue

            with self.lock:
                current_runtime, _, uid = heap_entry = heapq.heappop(self.queue)
                maybe_peer_id = self.experts.get(uid)
                if maybe_peer_id is None:
                    # remove expired expert from queue
                    self.uid_to_queue.pop(uid, None)
                    self.throughputs.pop(uid, None)
                if self.uid_to_queue.get(uid) != heap_entry:
                    continue  # skip uids that are banned or expired

                if self.throughputs[uid].num_updates != 0:
                    expected_time_taken = (
                        task_size / self.throughputs[uid].samples_per_second
                    )
                else:
                    expected_time_taken = self.initial_throughput * task_size
                new_heap_entry = (
                    current_runtime + expected_time_taken,
                    random.random(),
                    uid,
                )
                heapq.heappush(self.queue, new_heap_entry)
                self.uid_to_queue[uid] = new_heap_entry
                break
        try:
            with self.throughputs[uid].update_threadsafe(task_size):
                logger.debug(
                    f"Using expert {uid}, throughput = {self.throughputs[uid].samples_per_second}."
                )
                yield RemoteExpert(
                    ExpertInfo(uid, PeerID.from_base58(maybe_peer_id.value)), self._p2p
                )
        except BaseException:
            self._ban_expert(uid)
            raise

    def shutdown(self):
        self.is_alive.clear()
        self._trigger_updating_experts()


class NoModulesFound(RuntimeError):
    pass


class DiffusionClient:
    def __init__(
        self, *, initial_peers: List[str], dht_prefix: str = "diffusion", **kwargs
    ):
        dht = hivemind.DHT(initial_peers, client_mode=True, start=True, **kwargs)
        self.expert = BalancedRemoteExpert(dht=dht, uid_prefix=dht_prefix + ".")

    def draw(
        self, prompts: List[str], *, return_encoded: bool = False
    ) -> Union[np.ndarray, List[bytes]]:
        encoded_prompts = []
        for prompt in prompts:
            tensor = torch.tensor(list(prompt.encode()), dtype=torch.int64)
            tensor = F.pad(tensor, (0, MAX_PROMPT_LENGTH - len(tensor)))
            encoded_prompts.append(tensor)
        encoded_prompts = torch.stack(encoded_prompts)

        (encoded_images,) = self.expert(encoded_prompts)

        if return_encoded:
            return [buf.tobytes() for buf in encoded_images.numpy()]

        output_images = []
        for buf in encoded_images.numpy():
            image = cv2.imdecode(buf, 1)  # imdecode() returns a BGR image
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            output_images.append(image)
        return np.stack(output_images)

    @property
    def n_active_servers(self) -> int:
        return self.expert.expert_balancer.n_active_experts


class BalancedRemoteExpert(nn.Module):
    """
    A torch module that dynamically assigns weights to one RemoteExpert from a pool, proportionally to their throughput.
    ToDo docstring, similar to hivemind.RemoteExpert
    """

    def __init__(
        self,
        *,
        dht: hivemind.DHT,
        uid_prefix: str,
        grid_size: Tuple[int, ...] = (1, MAX_NODES),
        forward_timeout: Optional[float] = None,
        backward_timeout: Optional[float] = None,
        update_period: float = 30.0,
        backward_task_size_multiplier: float = 2.5,
        **kwargs,
    ):
        super().__init__()
        if uid_prefix.endswith(".0."):
            logger.warning(
                f"BalancedRemoteExperts will look for experts under prefix {self.uid_prefix}0."
            )
        assert len(grid_size) == 2 and grid_size[0] == 1, "only 1xN grids are supported"
        self.dht, self.uid_prefix, self.grid_size = dht, uid_prefix, grid_size
        self.forward_timeout, self.backward_timeout = forward_timeout, backward_timeout
        self.backward_task_size_multiplier = backward_task_size_multiplier
        self.expert_balancer = LoadBalancer(
            dht, key=f"{self.uid_prefix}0.", update_period=update_period, **kwargs
        )
        self._expert_info = None  # expert['info'] from one of experts in the grid

    def forward(self, *args: torch.Tensor, **kwargs: torch.Tensor):
        """
        Call one of the RemoteExperts for the specified inputs and return output. Compatible with pytorch.autograd.

        :param args: input tensors that will be passed to each expert after input, batch-first
        :param kwargs: extra keyword tensors that will be passed to each expert, batch-first
        :returns: averaged predictions of all experts that delivered result on time, nested structure of batch-first
        """
        assert len(kwargs) == len(
            self.info["keyword_names"]
        ), f"Keyword args should be {self.info['keyword_names']}"
        kwargs = {key: kwargs[key] for key in self.info["keyword_names"]}

        if self._expert_info is None:
            raise NotImplementedError()
        # Note: we put keyword arguments in the same order as on a server to prevent f(a=1, b=2) != f(b=2, a=1) errors

        forward_inputs = (args, kwargs)

        if not nested_compare(forward_inputs, self.info["forward_schema"]):
            raise TypeError(
                f"Inputs do not match expert input schema. Did you pass the right number of parameters?"
            )

        flat_inputs = list(nested_flatten(forward_inputs))
        forward_task_size = flat_inputs[0].shape[0]

        # Note: we send DUMMY to prevent torch from excluding expert from backward if no other inputs require grad
        flat_outputs = _BalancedRemoteModuleCall.apply(
            DUMMY,
            self.expert_balancer,
            self.info,
            self.forward_timeout,
            self.backward_timeout,
            forward_task_size,
            forward_task_size * self.backward_task_size_multiplier,
            *flat_inputs,
        )

        return nested_pack(flat_outputs, structure=self.info["outputs_schema"])

    @property
    def info(self):
        while self._expert_info is None:
            try:
                with self.expert_balancer.use_another_expert(1) as chosen_expert:
                    self._expert_info = chosen_expert.info
            except NoModulesFound:
                raise
            except Exception:
                logger.exception(
                    f"Tried to get expert info from {chosen_expert} but caught:"
                )
        return self._expert_info


class _BalancedRemoteModuleCall(torch.autograd.Function):
    """Internal autograd-friendly call of a remote module. For applications, use BalancedRemoteExpert instead."""

    @staticmethod
    def forward(
        ctx,
        dummy: torch.Tensor,
        expert_balancer: LoadBalancer,
        info: Dict[str, Any],
        forward_timeout: float,
        backward_timeout: float,
        forward_task_size: float,
        backward_task_size: float,
        *inputs: torch.Tensor,
    ) -> Tuple[torch.Tensor, ...]:
        # Note: *inputs are flattened input tensors that follow the expert's info['input_schema']
        # detach to avoid pickling the computation graph
        ctx.expert_balancer, ctx.info = expert_balancer, info
        ctx.forward_timeout, ctx.backward_timeout = forward_timeout, backward_timeout
        ctx.forward_task_size, ctx.backward_task_size = (
            forward_task_size,
            backward_task_size,
        )
        inputs = tuple(tensor.cpu().detach() for tensor in inputs)
        ctx.save_for_backward(*inputs)

        serialized_tensors = [
            serialize_torch_tensor(inp, proto.compression)
            for inp, proto in zip(inputs, nested_flatten(info["forward_schema"]))
        ]
        while True:
            try:
                with expert_balancer.use_another_expert(
                    forward_task_size
                ) as chosen_expert:
                    logger.info(f"Query served by: {chosen_expert}")
                    deserialized_outputs = RemoteExpertWorker.run_coroutine(
                        expert_forward(
                            chosen_expert.uid,
                            inputs,
                            serialized_tensors,
                            chosen_expert.stub,
                        )
                    )
                break
            except NoModulesFound:
                raise
            except Exception:
                logger.exception(
                    f"Tried to call forward for expert {chosen_expert} but caught:"
                )

        return tuple(deserialized_outputs)
