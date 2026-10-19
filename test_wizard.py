import unittest
from unittest.mock import patch
import tempfile
from pathlib import Path
import wizard as w

class InputsTest(unittest.TestCase):
    def test_multiple_preprocessing_inputs(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
            paths = []
            for name in ['one', 'two']:
                path = Path(directory) / (name + '.raw')
                path.write_text('label\n' + name + '\n')
                paths.append(str(path))
            meta = dict(source_type='csv', path=paths[0], sep=',', preprocess_enabled=True, preprocess_input_paths=paths)
            with patch.object(w, 'run_preprocessing_if_needed', side_effect=lambda meta, **kw: meta) as run:
                rows = w.parse_full_local(meta, {'Timestamp':'', 'Eventname':'label', 'Kommentar':''}, 'Event')
            self.assertEqual(rows['event_name'].tolist(), ['one','two'])
            self.assertEqual(run.call_count, 2)

    def test_dlt_decoder_and_range(self):
        def launch(command, stdout, stderr):
            self.assertEqual(command, ['converter path', '-a', '-b', '1', '-e', '50', 'trace path.dlt'])
            stdout.write(b'first message\nsecond message\n')
            from unittest.mock import Mock
            return Mock(returncode=0)
        with patch.object(w.subprocess, 'Popen', side_effect=launch):
            self.assertEqual(list(w.iter_dlt_rows('trace path.dlt','converter path',50)), [{'Message':1,'Text':'first message'}, {'Message':2,'Text':'second message'}])

    def test_dlt_missing_converter(self):
        with patch.object(w.subprocess, 'Popen', side_effect=OSError('missing')):
            with self.assertRaisesRegex(RuntimeError, 'DLT-Konverter'):
                list(w.iter_dlt_rows('trace.dlt'))

    def test_embedded_dlt_mapping(self):
        namespace = {}
        exec(w.EMBEDDED_PARSER_BLOCK, namespace)
        namespace['iter_dlt_rows'] = lambda *args: iter([{'Text':'decoded','Message':1}])
        rows = namespace['parse_rows']({'source_type':'dlt','path':'trace.dlt'}, {'Eventname':'Text'}, 'Event')
        self.assertEqual(rows[0]['event_name'],'decoded')

if __name__ == '__main__':
    unittest.main()

