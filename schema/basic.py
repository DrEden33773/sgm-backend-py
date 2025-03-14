from enum import StrEnum, auto
from typing import Any

STR_TUPLE_SPLITTER = "^"


class Op(StrEnum):
    Eq = "="
    Ne = "!="
    Gt = ">"
    Ge = ">="
    Lt = "<"
    Le = "<="


class AttrType(StrEnum):
    Int = "int"
    Float = "float"
    String = "string"


class VarPrefix(StrEnum):
    DataGraph = " "
    EnumerateTarget = "f"
    DbQueryTarget = "A"
    IntersectTarget = "T"
    IntersectCandidate = "C"
    DataVertexSet = "V"

    def __add__(self, other: Any):
        return self.value + STR_TUPLE_SPLITTER + str(other)


class InstructionType(StrEnum):
    Init = auto()
    """ BENU.INI """

    GetAdj = "get_adj"
    """ BENU.DBQ """

    Intersect = auto()
    """ BENU.INT """

    Foreach = auto()
    """ BENU.ENU """

    TCache = auto()
    """ BENU.TRC """

    Report = auto()
    """ BENU.RES """
