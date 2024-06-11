from textwrap import dedent
import unittest

from pyiron_snippets.factory import classfactory

from pyiron_workflow.channels import NOT_DATA
from pyiron_workflow.logging import logger
from pyiron_workflow.mixin.preview import ScrapesIO, no_output_validation_warning


class ScrapesFromDecorated(ScrapesIO):
    @classmethod
    def _io_defining_function(cls) -> callable:
        return cls._decorated_function


@classfactory
def scraper_factory(
    io_defining_function,
    validate_output_labels,
    io_defining_function_uses_self,
    /,
    *output_labels,
):
    return (
        io_defining_function.__name__,
        (ScrapesFromDecorated,),  # Define parentage
        {
            "_decorated_function": staticmethod(io_defining_function),
            "__module__": io_defining_function.__module__,
            "_output_labels": None if len(output_labels) == 0 else output_labels,
            "_validate_output_labels": validate_output_labels,
            "_io_defining_function_uses_self": io_defining_function_uses_self
        },
        {},
    )


def as_scraper(
    *output_labels,
    validate_output_labels=True,
    io_defining_function_uses_self=False,
):
    def scraper_decorator(fnc):
        scraper_factory.clear(fnc.__name__)  # Force a fresh class
        factory_made = scraper_factory(
            fnc, validate_output_labels, io_defining_function_uses_self, *output_labels
        )
        factory_made._class_returns_from_decorated_function = fnc
        factory_made.preview_io()
        return factory_made
    return scraper_decorator


class TestIOPreview(unittest.TestCase):
    # FROM FUNCTION
    def test_void(self):
        @as_scraper()
        def AbsenceOfIOIsPermissible():
            nothing = None

    def test_preview_inputs(self):
        @as_scraper()
        def Mixed(x, y: int = 42):
            """Has (un)hinted and with(out)-default input"""
            return x + y

        self.assertDictEqual(
            {"x": (None, NOT_DATA), "y": (int, 42)},
            Mixed.preview_inputs(),
            msg="Input specifications should be available at the class level, with or "
                "without type hints and/or defaults provided."
        )

        with self.subTest("Protected"):
            with self.assertRaises(
                ValueError,
                msg="Inputs must not overlap with __init__ signature terms"
            ):
                @as_scraper()
                def Selfish(self, x):
                    return x

    def test_preview_outputs(self):

        with self.subTest("Plain"):
            @as_scraper()
            def Return(x):
                return x

            self.assertDictEqual(
                {"x": None},
                Return.preview_outputs(),
                msg="Should parse without label or hint."
            )

        with self.subTest("Labeled"):
            @as_scraper("y")
            def LabeledReturn(x) -> None:
                return x

            self.assertDictEqual(
                {"y": type(None)},
                LabeledReturn.preview_outputs(),
                msg="Should parse with label and hint."
            )

        with self.subTest("Hint-return count mismatch"):
            with self.assertRaises(
                ValueError,
                msg="Should fail when scraping incommensurate hints and returns"
            ):
                @as_scraper()
                def HintMismatchesScraped(x) -> int:
                    y, z = 5.0, 5
                    return x, y, z

            with self.assertRaises(
                ValueError,
                msg="Should fail when provided labels are incommensurate with hints"
            ):
                @as_scraper("xo", "yo", "zo")
                def HintMismatchesProvided(x) -> int:
                    y, z = 5.0, 5
                    return x, y, z

        with self.subTest("Provided-scraped mismatch"):
            with self.assertRaises(
                ValueError,
                msg="The nuber of labels -- if explicitly provided -- must be commensurate "
                    "with the number of returned items"
            ):
                @as_scraper("xo", "yo")
                def LabelsMismatchScraped(x) -> tuple[int, float]:
                    y, z = 5.0, 5
                    return x

            @as_scraper("x0", "x1", validate_output_labels=False)
            def IgnoreScraping(x) -> tuple[int, float]:
                x = (5, 5.5)
                return x

            self.assertDictEqual(
                {"x0": int, "x1": float},
                IgnoreScraping.preview_outputs(),
                msg="Returned tuples can be received by force"
            )

        with self.subTest("Multiple returns"):
            with self.assertRaises(
                ValueError,
                msg="Branched returns cannot be scraped and will fail on validation"
            ):
                @as_scraper("truth")
                def Branched(x) -> bool:
                    if x <= 0:
                        return False
                    else:
                        return True

            @as_scraper("truth", validate_output_labels=False)
            def Branched(x) -> bool:
                if x <= 0:
                    return False
                else:
                    return True
            self.assertDictEqual(
                {"truth": bool},
                Branched.preview_outputs(),
                msg="We can force-override this at our own risk."
            )

        with self.subTest("Uninspectable function"):
            def _uninspectable():
                template = dedent(f"""
                    def __source_code_not_available(x):
                        return x
                """)
                exec(template)
                return locals()["__source_code_not_available"]

            f = _uninspectable()

            with self.assertRaises(
                OSError,
                msg="If the source code cannot be inspected for output labels, they "
                    "_must_ be provided."
            ):
                as_scraper()(f)

            with self.assertLogs(logger.name, level="WARNING") as log:
                new_cls = as_scraper("y")(f)

            self.assertIn(
                f"WARNING:{logger.name}:" + no_output_validation_warning(new_cls),
                log.output,
                msg="Verify that the expected warning appears in the log"
            )
