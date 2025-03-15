from typing import Type

from executor.instr_ops.abc import InstrOperator
from executor.matching_ctx import MatchingCtx
from schema import Instruction, InstructionType
from storage.abc import StorageAdapter


class OperatorFactory:
    """指令算子工厂"""

    _operators: dict[InstructionType, Type[InstrOperator]] = {}

    @classmethod
    def register(cls, instr_type: InstructionType):
        """注册指令算子"""

        def decorator(operator_cls: Type[InstrOperator]) -> Type[InstrOperator]:
            cls._operators[instr_type] = operator_cls
            return operator_cls

        return decorator

    @classmethod
    def create(
        cls,
        instr: Instruction,
        storage_adapter: StorageAdapter,
        ctx: MatchingCtx,
    ) -> InstrOperator:
        """创建指令算子"""

        operator_cls = cls._operators.get(instr.type)
        if operator_cls is None:
            raise ValueError(f"Unknown instruction type: {instr.type}")
        return operator_cls(storage_adapter, ctx)
