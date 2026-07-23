from __future__ import annotations

import operator
import unittest

from unit import _fixtures

from pyiron_workflow._wfms import constructors, workflow


def _only(mapping):
    """Return the single key in a one-element port map."""
    keys = list(mapping)
    assert len(keys) == 1, f"expected exactly one entry, got {keys}"
    return keys[0]


def _new_node():
    """A fresh single-output Atomic wrapping `plain_increment` (x -> x + 1)."""
    return constructors.node(_fixtures.plain_increment)


class _Mat:
    """Minimal object supporting `@`; returns a sentinel so the result is exact."""

    def __init__(self, v):
        self.v = v

    def __matmul__(self, other):
        return ("mm", self.v, other.v)


def _run_binary(label, op, a_val, b_val):
    """Build a workflow with inputs a, b; inject `op(a, b)` wired to output `out`;
    return the run value. `op` is an `operator` function so `op(port_a, port_b)`
    invokes the corresponding port dunder."""
    wf = workflow.Workflow(label)
    wf.create_input("a")
    wf.create_input("b")
    wf.create_output("out")
    wf.r = op(wf.inputs.a, wf.inputs.b)
    wf.connect(wf.r, wf.outputs.out)
    return wf.run(a=a_val, b=b_val).outputs.out


class TestUnaryInjection(unittest.TestCase):
    def test_free_unary_on_single_output_node(self):
        result = abs(_new_node())
        self.assertIsInstance(result, workflow.Workflow)
        # operation node + absorbed source node
        self.assertEqual(2, len(result.nodes))
        in_label = _only(result.inputs)
        out_label = _only(result.outputs)
        value = result.run(**{in_label: -5}).outputs[out_label]
        self.assertEqual(abs(-5 + 1), value)  # abs(-4) == 4

    def test_unary_on_input_port(self):
        wf = workflow.Workflow("unary_input")
        wf.create_input("n")
        wf.create_output("out")
        wf.absn = abs(wf.inputs.n)
        wf.connect(wf.absn, wf.outputs.out)
        self.assertEqual(42, wf.run(n=-42).outputs.out)

    def test_unary_on_output_port(self):
        wf = workflow.Workflow("unary_output")
        wf.create_input("n")
        wf.create_output("out")
        wf.first = _new_node()
        wf.connect(wf.inputs.n, wf.first.inputs.x)
        wf.absf = abs(wf.first.outputs.output_0)
        wf.connect(wf.absf, wf.outputs.out)
        self.assertEqual(42, wf.run(n=-43).outputs.out)  # abs(-43 + 1) == 42

    def test_owned_unary(self):
        wf = workflow.Workflow("owned_unary")
        wf.create_input("n")
        wf.create_output("out")
        wf.first = _new_node()
        wf.absf = abs(wf.first)
        wf.connect(wf.inputs.n, wf.first.inputs.x)
        wf.connect(wf.absf, wf.outputs.out)
        self.assertEqual(42, wf.run(n=-43).outputs.out)

    def test_repeated_injection_unique_labels(self):
        wf = workflow.Workflow("repeated_unary")
        wf.first = _new_node()
        wf.a = abs(wf.first)
        wf.b = abs(wf.first)
        self.assertIsNot(wf.a, wf.b)
        self.assertNotEqual(wf.a.label, wf.b.label)


class TestBinaryInjection(unittest.TestCase):
    def test_doubly_owned_binary_runs_independently(self):
        wf = workflow.Workflow("doubly_owned")
        wf.first = _new_node()
        wf.second = _new_node()
        result = wf.first + wf.second
        self.assertIsInstance(result, workflow.Workflow)
        # The inputs are the OUTPUT PORTS of the owned source nodes fed as plumbing
        # inputs to the add operation. Running result directly feeds those plumbing
        # inputs; the increment nodes inside wf do not execute again here.
        out_label = _only(result.outputs)
        value = result.run(first=1, second=2).outputs[out_label]
        self.assertEqual(1 + 2, value)  # 3

    def test_binary_naming_node_vs_output_port(self):
        wf = workflow.Workflow("naming")
        wf.first = _new_node()
        wf.second = _new_node()

        by_node = wf.first + wf.second
        by_port = wf.first.outputs.output_0 + wf.second.outputs.output_0

        node_out = _only(by_node.outputs)
        port_out = _only(by_port.outputs)
        self.assertEqual(3, by_node.run(first=1, second=2).outputs[node_out])
        self.assertEqual(
            3,
            by_port.run(first_output_0=1, second_output_0=2).outputs[port_out],
        )

    def test_binary_on_input_ports(self):
        wf = workflow.Workflow("binary_inputs")
        wf.create_input("m")
        wf.create_input("n")
        wf.create_output("product")
        wf.prod = wf.inputs.m * wf.inputs.n
        wf.connect(wf.prod, wf.outputs.product)
        self.assertEqual(6, wf.run(m=2, n=3).outputs.product)

    def test_self_binary_same_port(self):
        wf = workflow.Workflow("self_binary")
        wf.create_input("m")
        wf.create_output("out")
        wf.doubled = wf.inputs.m + wf.inputs.m
        wf.connect(wf.doubled, wf.outputs.out)
        self.assertEqual(8, wf.run(m=4).outputs.out)


class TestMixedOwnership(unittest.TestCase):
    def test_full_mixed_pipeline(self):
        # lin is increment(m) multiplied by x; y is lin plus increment(b).
        # run(m=1, x=2, b=0.5) gives ((1+1)*2) + (0.5+1) == 5.5
        wf = workflow.Workflow("mixed_ownership")
        wf.create_input("m")
        wf.create_input("x")
        wf.create_input("b")
        wf.lin = _new_node() * wf.inputs.x
        # lin inputs: plain_increment_0_x (free), x (owned)
        wf.connect(wf.inputs.m, wf.lin.inputs["plain_increment_0_x"])
        wf.y = wf.lin + _new_node()
        # y inputs: lin (owned via pending edge), plain_increment_0_x (free)
        wf.connect(wf.inputs.b, wf.y.inputs["plain_increment_0_x"])
        wf.create_output("y")
        wf.connect(wf.y, wf.outputs.y)
        self.assertEqual(5.5, wf.run(m=1, x=2, b=0.5).outputs.y)

    def test_right_owned_left_free(self):
        # (increment(k) + b) with b owned, increment free.
        wf = workflow.Workflow("right_owned")
        wf.create_input("b")
        wf.create_input("k")
        wf.create_output("out")
        free = _new_node()
        wf.y = free + wf.inputs.b
        # y inputs: plain_increment_0_x (free), b (owned)
        wf.connect(wf.inputs.k, wf.y.inputs["plain_increment_0_x"])
        wf.connect(wf.y, wf.outputs.out)
        self.assertEqual(
            (5 + 1) + 10,
            wf.run(k=5, b=10).outputs.out,
        )


class TestChainedInjection(unittest.TestCase):
    def test_chained_with_context_builds_and_runs(self):
        wf = workflow.Workflow("chained_with_context")
        wf.create_input("m")
        wf.create_input("x")
        wf.create_input("b")
        wf.y = (wf.inputs.m * wf.inputs.x) + wf.inputs.b
        wf.create_output("result")
        wf.connect(wf.y, wf.outputs.result)
        self.assertEqual((2 * 3) + 4, wf.run(m=2, x=3, b=4).outputs.result)

    def test_no_context_chain_still_works(self):
        # Regression: chaining unowned nodes must keep working.
        # Real labels discovered at runtime:
        #   plain_increment_mul_plain_increment_0_plain_increment_0_x  (left mul operand)
        #   plain_increment_mul_plain_increment_0_plain_increment_1_x  (right mul operand)
        #   plain_increment_0_x                                        (add's right operand)
        chained = (_new_node() * _new_node()) + _new_node()
        out_label = _only(chained.outputs)
        self.assertEqual(
            7.5,
            chained.run(
                plain_increment_mul_plain_increment_0_plain_increment_0_x=1,
                plain_increment_mul_plain_increment_0_plain_increment_1_x=2,
                plain_increment_0_x=0.5,
            ).outputs[out_label],
        )

    def test_cross_context_via_pending_rejected_early(self):
        wf = workflow.Workflow("one_owner")
        wf.create_input("m")
        wf.create_input("x")
        wf2 = workflow.Workflow("another_owner")
        wf2.create_input("b")
        with self.assertRaises(ValueError):
            (wf.inputs.m * wf.inputs.x) + wf2.inputs.b


class TestPendingConnectionLifting(unittest.TestCase):
    def test_absorb_source_with_pending_connection(self):
        # n1 -> n2 (pending, both free), then abs(n2) must lift n2's pending edge onto
        # the new graph.  The lifted edge resolves when the new graph is attached to an
        # outer workflow that also owns n1.
        n1 = _new_node()
        n2 = _new_node()
        n2(n1)  # connect_input: caches a pending connection on the free n2
        result = abs(n2)
        self.assertIsInstance(result, workflow.Workflow)
        # n2 is absorbed; result has a pending connection 'plain_increment_0_x' <- n1.output_0
        # To realise the full n1 -> n2 -> abs chain, attach both to an outer workflow.
        outer = workflow.Workflow("outer_lifting")
        outer.create_input("x")
        outer.create_output("out")
        outer.n1 = n1
        outer.connect(outer.inputs.x, outer.n1.inputs.x)
        outer.sub = (
            result  # pending edge n1.output_0 -> sub.plain_increment_0_x resolves
        )
        outer.connect(outer.sub, outer.outputs.out)
        # abs((x+1)+1) with x=-7 -> abs(-5) == 5
        self.assertEqual(5, outer.run(x=-7).outputs.out)


class TestInjectionFailures(unittest.TestCase):
    def test_cross_context_binary_direct(self):
        wf = workflow.Workflow("ctx_a")
        wf.create_input("a")
        wf2 = workflow.Workflow("ctx_b")
        wf2.create_input("b")
        with self.assertRaisesRegex(ValueError, "across graph contexts"):
            wf.inputs.a + wf2.inputs.b

    def test_multi_output_node_rejected(self):
        macro = _fixtures.macro_node()  # outputs: a, s  (>1 output)
        with self.assertRaisesRegex(ValueError, "more than one output port"):
            abs(macro)

    def test_add_injection_graph_to_wrong_workflow(self):
        wf = workflow.Workflow("owner")
        wf.first = _new_node()
        wf.second = _new_node()
        elsewise = wf.first + wf.second
        wf2 = workflow.Workflow("stranger")
        with self.assertRaisesRegex(ValueError, "not owned"):
            wf2.added = elsewise

    def test_source_node_removed_breaks_attachment(self):
        wf = workflow.Workflow("owner_rm")
        wf.first = _new_node()
        wf.second = _new_node()
        elsewise = wf.first + wf.second
        wf.remove_node("first")
        with self.assertRaisesRegex(ValueError, "not owned"):
            wf.added = elsewise
        wf.undo()  # restore removed node

    def test_source_node_renamed_then_attaches(self):
        # Pending edges are by object reference, so renaming does not break attachment.
        wf = workflow.Workflow("owner_rn")
        wf.create_input("a")
        wf.create_input("b_in")
        wf.create_output("out")
        wf.first = _new_node()
        wf.second = _new_node()
        elsewise = wf.first + wf.second
        wf.rename_node(wf.first, "fiiiiirst")
        wf.added = elsewise  # succeeds: pending edges by object reference
        # Wire the child node inputs through the outer workflow inputs
        wf.connect(wf.inputs.a, wf.fiiiiirst.inputs.x)
        wf.connect(wf.inputs.b_in, wf.second.inputs.x)
        wf.connect(wf.added, wf.outputs.out)
        self.assertEqual(
            (1 + 1) + (2 + 1),
            wf.run(a=1, b_in=2).outputs.out,
        )


class TestInjectionTypeHints(unittest.TestCase):
    def test_hints_propagate_to_generated_ports(self):
        node = _fixtures.typed_int_node()  # input x: int, output output_0: int
        result = abs(node)
        in_label = _only(result.inputs)
        out_label = _only(result.outputs)
        # The source-derived plumbing input carries the type_hint from the source node's
        # input port.
        self.assertEqual(int, result.inputs[in_label].type_hint)
        # The output hint reflects the abs operation's own recipe output typing,
        # which is None (the std recipe is untyped).
        self.assertIsNone(result.outputs[out_label].type_hint)


class TestImmutableContext(unittest.TestCase):
    def test_inject_on_immutable_context_raises(self):
        # A child node inside a Macro has an injection context that is the Macro itself
        # (an ImmutableDag, not a MutableDag) -> TypeError from
        # InjectionContext._validate_injection_context_graph.
        macro = _fixtures.macro_node()
        child = next(iter(macro.nodes.values()))
        child_out = next(iter(child.outputs.values()))
        with self.assertRaises(TypeError):
            abs(child_out)


class TestGetitemOperator(unittest.TestCase):
    def test_getitem_port_index(self):
        # container[index] with both supplied as ports -> indexed element
        self.assertEqual(20, _run_binary("getitem", operator.getitem, [10, 20, 30], 1))

    def test_literal_index_supported(self):
        # A JSONable literal index is wrapped in a Constant and works.
        wf = workflow.Workflow("getitem_literal")
        wf.create_input("container")
        wf.create_output("out")
        wf.r = wf.inputs.container[1]
        wf.connect(wf.r, wf.outputs.out)
        self.assertEqual(20, wf.run(container=[10, 20, 30]).outputs.out)

    def test_non_jsonable_index_rejected(self):
        # A non-JSONable index (a tuple) is neither injectable nor JSONable.
        wf = workflow.Workflow("getitem_bad")
        wf.create_input("container")
        with self.assertRaisesRegex(TypeError, "getitem"):
            wf.inputs.container[(1, 2)]


class TestMatmulOperator(unittest.TestCase):
    def test_matmul(self):
        self.assertEqual(
            ("mm", 1, 2),
            _run_binary("matmul", operator.matmul, _Mat(1), _Mat(2)),
        )


class TestBitwiseOperators(unittest.TestCase):
    def test_and(self):
        self.assertEqual(2, _run_binary("and_", operator.and_, 6, 3))

    def test_or(self):
        self.assertEqual(7, _run_binary("or_", operator.or_, 6, 1))

    def test_xor(self):
        self.assertEqual(5, _run_binary("xor", operator.xor, 6, 3))

    def test_lshift(self):
        self.assertEqual(8, _run_binary("lshift", operator.lshift, 1, 3))

    def test_rshift(self):
        self.assertEqual(2, _run_binary("rshift", operator.rshift, 8, 2))


class TestArithmeticOperators(unittest.TestCase):
    def test_sub(self):
        self.assertEqual(2, _run_binary("sub", operator.sub, 5, 3))

    def test_truediv(self):
        self.assertEqual(3.0, _run_binary("truediv", operator.truediv, 6, 2))

    def test_floordiv(self):
        self.assertEqual(3, _run_binary("floordiv", operator.floordiv, 7, 2))

    def test_mod(self):
        self.assertEqual(1, _run_binary("mod", operator.mod, 7, 3))

    def test_pow(self):
        self.assertEqual(8, _run_binary("pow", operator.pow, 2, 3))


class TestUnaryOperators(unittest.TestCase):
    def test_neg(self):
        wf = workflow.Workflow("neg")
        wf.create_input("a")
        wf.create_output("out")
        wf.r = -wf.inputs.a
        wf.connect(wf.r, wf.outputs.out)
        self.assertEqual(-5, wf.run(a=5).outputs.out)

    def test_pos(self):
        wf = workflow.Workflow("pos")
        wf.create_input("a")
        wf.create_output("out")
        wf.r = +wf.inputs.a
        wf.connect(wf.r, wf.outputs.out)
        self.assertEqual(-5, wf.run(a=-5).outputs.out)

    def test_invert(self):
        wf = workflow.Workflow("invert")
        wf.create_input("a")
        wf.create_output("out")
        wf.r = ~wf.inputs.a
        wf.connect(wf.r, wf.outputs.out)
        self.assertEqual(-6, wf.run(a=5).outputs.out)  # ~5 == -6


_CONSTANT_BINARY_CASES = [
    # (name, build(port), a_val, expected). Covers forward (literal-on-right) and
    # reflected (literal-on-left) forms; reflected arithmetic proves operand order.
    ("add_right", lambda a: a + 3, 5, 8),
    ("add_left", lambda a: 3 + a, 5, 8),
    ("sub_right", lambda a: a - 3, 10, 7),
    ("sub_left", lambda a: 3 - a, 10, -7),
    ("mul_right", lambda a: a * 3, 4, 12),
    ("mul_left", lambda a: 3 * a, 4, 12),
    ("truediv_right", lambda a: a / 4, 10, 2.5),
    ("truediv_left", lambda a: 20 / a, 4, 5.0),
    ("floordiv_right", lambda a: a // 3, 10, 3),
    ("floordiv_left", lambda a: 20 // a, 3, 6),
    ("mod_right", lambda a: a % 3, 10, 1),
    ("mod_left", lambda a: 20 % a, 3, 2),
    ("pow_right", lambda a: a**2, 3, 9),
    ("pow_left", lambda a: 2**a, 3, 8),
    ("lshift_right", lambda a: a << 2, 1, 4),
    ("lshift_left", lambda a: 1 << a, 3, 8),
    ("rshift_right", lambda a: a >> 1, 8, 4),
    ("rshift_left", lambda a: 32 >> a, 2, 8),
    ("and_right", lambda a: a & 3, 6, 2),
    ("and_left", lambda a: 3 & a, 6, 2),
    ("or_right", lambda a: a | 1, 6, 7),
    ("or_left", lambda a: 1 | a, 6, 7),
    ("xor_right", lambda a: a ^ 3, 6, 5),
    ("xor_left", lambda a: 3 ^ a, 6, 5),
]


class TestConstantInjection(unittest.TestCase):
    """Binary injection where one operand is a JSONable literal."""

    @staticmethod
    def _run(build, a_val):
        wf = workflow.Workflow("const_inj")
        wf.create_input("a")
        wf.create_output("out")
        wf.r = build(wf.inputs.a)
        wf.connect(wf.r, wf.outputs.out)
        return wf.run(a=a_val).outputs.out

    def test_binary_literal_operands(self):
        for name, build, a_val, expected in _CONSTANT_BINARY_CASES:
            with self.subTest(name):
                self.assertEqual(expected, self._run(build, a_val))

    def test_reflected_arithmetic_is_not_commuted(self):
        # Guards the operand-order contract: `2 - port` must not equal `port - 2`.
        self.assertNotEqual(
            self._run(lambda a: 2 - a, 10), self._run(lambda a: a - 2, 10)
        )

    def test_float_constant(self):
        self.assertEqual(6.0, self._run(lambda a: a * 1.5, 4))

    def test_list_constant(self):
        self.assertEqual([1, 9], self._run(lambda a: a + [9], [1]))

    def test_owned_context_chained_literals(self):
        # Literal on the right, then on the left, both within the same graph context.
        wf = workflow.Workflow("owned_const")
        wf.create_input("a")
        wf.create_output("out")
        wf.chained = 3 ** (wf.inputs.a * 2)
        wf.connect(wf.chained, wf.outputs.out)
        self.assertEqual(3 ** (2 * 2), wf.run(a=2).outputs.out)  # 81

    def test_free_node_with_literal(self):
        result = _new_node() * 2  # plain_increment(x) then * 2
        self.assertIsInstance(result, workflow.Workflow)
        in_label = _only(result.inputs)
        out_label = _only(result.outputs)
        self.assertEqual((7 + 1) * 2, result.run(**{in_label: 7}).outputs[out_label])

    def test_reflected_matmul_builds_graph(self):
        # matmul of two constants is not runnable, but the reflected dunder must still
        # build an injection graph (int has no __matmul__, so `2 @ port` dispatches here).
        wf = workflow.Workflow("rmm")
        wf.create_input("a")
        result = 2 @ wf.inputs.a
        self.assertIsInstance(result, workflow.Workflow)

    def test_non_jsonable_right_operand_rejected(self):
        wf = workflow.Workflow("bad_right")
        wf.create_input("a")
        with self.assertRaisesRegex(TypeError, "mul"):
            wf.inputs.a * (2,)

    def test_non_jsonable_left_operand_rejected(self):
        wf = workflow.Workflow("bad_left")
        wf.create_input("a")
        with self.assertRaisesRegex(TypeError, "mul"):
            (2,) * wf.inputs.a


if __name__ == "__main__":
    unittest.main()
