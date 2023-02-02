from unittest import TestCase

from pyiron_contrib.workflow.channels import ChannelTemplate, InputChannel, OutputChannel


class DummyNode:
    def update(self):
        pass


class TestIO(TestCase):
    def test_channels(self):
        num_channel = ChannelTemplate(default=1, types=[int, float])
        # Note: We intentionally violate the type hinting and give a *mutable* _list_ of
        #       types instead of an immutable _tuple_ of types
        str_list_channel = ChannelTemplate(default=["foo"], types=list)
        ni1 = num_channel.to_input(DummyNode())
        ni2 = num_channel.to_input(DummyNode())
        no = num_channel.to_output(DummyNode())

        with self.subTest("Validate `to_X` typing"):
            self.assertIsInstance(ni1, InputChannel)
            self.assertIsInstance(no, OutputChannel)

        so1 = str_list_channel.to_output(DummyNode())
        so2 = str_list_channel.to_output(DummyNode())

        with self.subTest("Ensure that types are not treated mutably"):
            # We intentionally passed the wrong type at instantiation, let's make sure
            # this can't get us into trouble
            self.assertIsInstance(ni1.types, tuple)

        with self.subTest("Ensure that mutable defaults aren't shared among instances"):
            so1.default.append("bar")
            self.assertEqual(len(so2.default), len(so1.default) - 1)

        with self.subTest("Test connection reflexivity"):
            ni1.connect(no)
            self.assertIn(no, ni1.connections)
            self.assertIn(ni1, no.connections)

        with self.subTest("Test disconnection"):
            ni2.disconnect(no)  # Should do nothing
            ni1.disconnect(no)
            self.assertEqual([], ni1.connections, msg="No connections should be left")
            self.assertEqual(
                [], no.connections, msg="Disconnection should also have been reflexive"
            )

        with self.subTest("Test connection validity"):
            ni1.types = (int, float, bool)  # Override with a larger set
            ni2.types = (int,)  # Override with a smaller set

            no.connect(ni1)
            self.assertIn(
                no,
                ni1.connections,
                "Input types should be allowed to be a super-set of output types"
            )

            no.connect(ni2)
            self.assertNotIn(
                no,
                ni2.connections,
                "Input types should not be allowed to be a sub-set of output types"
            )

            so1.connect(ni2)
            self.assertNotIn(
                so1,
                ni2.connections,
                "Totally different types should not allow connections"
            )

        with self.subTest("Test value readiness"):
            no.value = 1
            self.assertTrue(no.ready)
            no.value = "Not numeric at all"
            self.assertFalse(no.ready)

        with self.subTest("Test update message passing"):
            assert(ni1 in no.connections)  # Internal check for test structure
            # Should still be connected from earlier
            no.update(42)
            self.assertEqual(42, ni1.value)
