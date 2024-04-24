from abc import ABC
import pickle
import unittest

from pyiron_workflow.snippets.constructed import Constructed, mix_and_construct_instance


class HasN(ABC):
    def __init_subclass__(cls, /, n=0, s="foo", **kwargs):
        super(HasN, cls).__init_subclass__(**kwargs)
        cls.n = n
        cls.s = s

    def __init__(self, x, y=0):
        self.x = x
        self.y = y


def has_n_factory(n, s="bar"):
    return type(
        f"{HasN.__name__}{n}",
        (HasN,),
        {"__module__": HasN.__module__},
        n=n,
        s=s
    )


class Constructable(Constructed, HasN, ABC):
    pass


def constructable_factory(n, s="baz"):
    return type(
        f"{Constructable.__name__}{n}",
        (Constructable,),
        {},
        n=n,
        s=s,
        class_factory=constructable_factory,
        class_factory_args=(n,),
        class_factory_kwargs={"s": s},
        class_instance_args=(0,)
    )


class TestConstructed(unittest.TestCase):
    def test_reduce(self):
        by_inheritance = constructable_factory(1)(2)
        constructor, constructor_args, state = by_inheritance.__reduce__()
        f, f_args, f_kwargs, i_args, i_kwargs, sc_kwargs = constructor_args

        self.assertIs(Constructed._instance_constructor, constructor)
        self.assertIs(constructable_factory, f)
        self.assertTupleEqual((1,), f_args)
        self.assertDictEqual({"s": "baz"}, f_kwargs)  # from the factory defaults
        self.assertTupleEqual((0,), i_args)  # from the factory
        self.assertDictEqual({}, i_kwargs)  # from Constructed
        self.assertDictEqual({}, sc_kwargs)  # from Constructed

    def test_inheritance_mixin(self):
        by_inheritance = constructable_factory(42)(43, y=44)
        self.assertTupleEqual(
            (42, "baz", 43, 44),
            (by_inheritance.n, by_inheritance.s, by_inheritance.x, by_inheritance.y),
            msg="Sanity check."
        )
        reloaded = pickle.loads(pickle.dumps(by_inheritance))
        self.assertTupleEqual(
            (by_inheritance.n, by_inheritance.s, by_inheritance.x, by_inheritance.y),
            (reloaded.n, reloaded.s, reloaded.x, reloaded.y),
            msg=f"Children of {Constructed.__name__} should recover both class and "
                f"instance state under the (un)pickle cycle."
        )

    def test_instantiation_mixin(self):
        dynamic_mixin = mix_and_construct_instance(
            has_n_factory,
            (42,),
            {"s": "baz"},
            (43,),
            {"y": 44},
            {"n": 42, "s": "baz"},  # __init_subclass__ kwargs get duplicated here
            # This is annoying for users of `mix_and_construct_instance`, but not
            # difficult.
        )
        self.assertIsInstance(
            dynamic_mixin,
            Constructed,
            msg=f"{mix_and_construct_instance.__name__} should dynamically add "
                f"{Constructed.__name__} inheritance."
        )
        self.assertTupleEqual(
            (42, "baz", 43, 44),
            (dynamic_mixin.n, dynamic_mixin.s, dynamic_mixin.x, dynamic_mixin.y),
            msg="Sanity check."
        )

        reloaded = pickle.loads(pickle.dumps(dynamic_mixin))
        self.assertTupleEqual(
            (dynamic_mixin.n, dynamic_mixin.s, dynamic_mixin.x, dynamic_mixin.y),
            (reloaded.n, reloaded.s, reloaded.x, reloaded.y),
            msg=f"Children of {Constructed.__name__} should recover both class and "
                f"instance state under the (un)pickle cycle."
        )

