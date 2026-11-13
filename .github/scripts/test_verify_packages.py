import io
import unittest
from zipfile import ZipFile

from verify_packages import verify_analyzers


class AnalyzerPackageTests(unittest.TestCase):
    def verify(self, *, loose=False, missing=None):
        files = ["Relayer.SourceGenerators.dll", "Relayer.CodeFixes.dll",
                 "System.Text.Json.SourceGeneration.LICENSE.txt",
                 "System.Text.Json.SourceGeneration.NOTICES.txt"]
        if loose:
            files.append("System.Text.Json.SourceGeneration.dll")
        buffer = io.BytesIO()
        with ZipFile(buffer, "w") as archive:
            for name in files:
                if name != missing:
                    archive.writestr("analyzers/dotnet/cs/" + name, b"content")
        with ZipFile(buffer) as archive:
            verify_analyzers(archive)

    def test_embedded_generator_package_layout(self):
        self.verify()

    def test_rejects_loose_generator(self):
        with self.assertRaisesRegex(AssertionError, "must be embedded"):
            self.verify(loose=True)

    def test_requires_analyzers_and_notices(self):
        for name in ("Relayer.SourceGenerators.dll", "Relayer.CodeFixes.dll",
                     "System.Text.Json.SourceGeneration.LICENSE.txt",
                     "System.Text.Json.SourceGeneration.NOTICES.txt"):
            with self.subTest(name=name), self.assertRaises((AssertionError, KeyError)):
                self.verify(missing=name)


if __name__ == "__main__":
    unittest.main()
