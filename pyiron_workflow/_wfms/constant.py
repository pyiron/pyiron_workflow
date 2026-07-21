from __future__ import annotations

import flowrep as fr

from pyiron_workflow._wfms import execution
from pyiron_workflow._wfms.datatypes import StaticNode


class Constant(StaticNode[fr.schemas.ConstantRecipe, fr.schemas.ConstantData]):

    @classmethod
    def from_value(cls, value: fr.schemas.JSON):
        return cls("constant", fr.schemas.ConstantRecipe(constant=value))

    @classmethod
    def _result_type(cls) -> type[fr.schemas.AtomicData]:
        return fr.schemas.ConstantData

    def evaluate(
        self,
        run: execution.Run[execution.ResultType],
        config: execution.RunConfig,
    ) -> execution.Run[execution.ResultType]:
        run.result = fr.tools.recipe2data(self.recipe)
        return run
