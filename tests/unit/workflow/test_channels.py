from unittest import TestCase

from pyiron_contrib.workflow.channels import ChannelTemplate, InputChannel, OutputChannel


class DummyNode:
    def update(self):
        pass


class TestChannels(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.num_channel = ChannelTemplate(name="numeric", default=1, types=[int, float])
        # Note: We intentionally violate the type hinting and give a *mutable* _list_ of
        #       types instead of an immutable _tuple_ of types
        cls.str_list_channel = ChannelTemplate(name="list", default=["foo"], types=list)

    def setUp(self) -> None:
        self.ni1 = self.num_channel.to_input(DummyNode())
        self.ni2 = self.num_channel.to_input(DummyNode())
        self.no = self.num_channel.to_output(DummyNode())

        self.so1 = self.str_list_channel.to_output(DummyNode())
        self.so2 = self.str_list_channel.to_output(DummyNode())

    def test_name(self):
        self.assertEqual(self.num_channel.name, self.ni1.name)
        self.assertEqual(self.num_channel.name, self.no.name)

    def test_template_conversion(self):
        self.assertIsInstance(self.ni1, InputChannel)
        self.assertIsInstance(self.no, OutputChannel)

    def test_type_tuple_conversion(self):
        # We intentionally passed the wrong type at instantiation, let's make sure
        # this can't get us into trouble
        self.assertIsInstance(self.ni1.types, tuple)
        self.assertIsInstance(self.ni2.types, tuple)

    def test_mutable_defaults(self):
        self.so1.default.append("bar")
        self.assertEqual(
            len(self.so2.default),
            len(self.so1.default) - 1,
            msg="Mutable defaults should avoid sharing between instances"
        )

    def test_connections(self):

        with self.subTest("Test connection reflexivity"):
            self.ni1.connect(self.no)
            self.assertIn(self.no, self.ni1.connections)
            self.assertIn(self.ni1, self.no.connections)

        with self.subTest("Test disconnection"):
            self.ni2.disconnect(self.no)  # Should do nothing
            self.ni1.disconnect(self.no)
            self.assertEqual(
                [], self.ni1.connections, msg="No connections should be left"
            )
            self.assertEqual(
                [],
                self.no.connections,
                msg="Disconnection should also have been reflexive"
            )

        with self.subTest("Test multiple connections"):
            self.no.connect(self.ni1, self.ni2)
            self.assertEqual(2, len(self.no.connections), msg="Should connect to all")

    def test_connection_validity_tests(self):
        self.ni1.types = (int, float, bool)  # Override with a larger set
        self.ni2.types = (int,)  # Override with a smaller set

        with self.assertRaises(TypeError):
            self.ni1.connect("Not a channel at all")

        self.no.connect(self.ni1)
        self.assertIn(
            self.no,
            self.ni1.connections,
            "Input types should be allowed to be a super-set of output types"
        )

        self.no.connect(self.ni2)
        self.assertNotIn(
            self.no,
            self.ni2.connections,
            "Input types should not be allowed to be a sub-set of output types"
        )

        self.so1.connect(self.ni2)
        self.assertNotIn(
            self.so1,
            self.ni2.connections,
            "Totally different types should not allow connections"
        )

    def test_ready(self):
        self.no.value = 1
        self.assertTrue(self.no.ready)
        self.no.value = "Not numeric at all"
        self.assertFalse(self.no.ready)

    def test_update(self):
        self.no.connect(self.ni1, self.ni2)
        self.no.update(42)
        for inp in self.no.connections:
            self.assertEqual(
                self.no.value,
                inp.value,
                msg="Value should have been passed downstream"
            )
