import glob
import shutil
import subprocess
import sys
import tempfile

import p223_pdf_to_csv

pdftotext = shutil.which('pdftotext')
if pdftotext is None:
	sys.exit("'pdftotext' is not installed. See installation instructions in README.md.")

tr = shutil.which('tr')
if tr is None:
	sys.exit("'tr' is not installed. See installation instructions in README.md.")

pdf_paths = glob.glob(f'../input/*.pdf')

pdf_path = pdf_paths[0]

with tempfile.NamedTemporaryFile(delete_on_close=False) as squished_tempfile:
	# '-f 2' to start processing on the second page of the PDF (since page 1 is a cover page).
	pdftotext_process = subprocess.Popen([pdftotext, '-layout', pdf_path, '-f', '2', '-'], stdout=subprocess.PIPE)
	tr_process = subprocess.Popen([tr, '-s', "' '"], stdin=pdftotext_process.stdout, stdout=squished_tempfile)
	pdftotext_process.stdout.close()
	out, err = tr_process.communicate()

	if err is not None:
		sys.exit(f"'pdftotext' and/or 'tr' had an error:\n{err}")

	if out is not None:
		print(f"'pdftotext' and/or 'tr' produced output on stdout, which was unexpected:\n{out}")
		
	breakpoint()

	squished_tempfile.close()

	output_file = '../output/out.csv'
	p223_pdf_to_csv.main(squished_tempfile.name, output_file)

print("All done")