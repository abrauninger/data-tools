import glob
import shutil
import subprocess
import sys

pdftotext = shutil.which('pdftotext')
if pdftotext is None:
	sys.exit("'pdftotext' is not installed. See installation instructions in README.md.")

tr = shutil.which('tr')
if tr is None:
	sys.exit("'tr' is not installed. See installation instructions in README.md.")

pdf_paths = glob.glob(f'../input/*.pdf')

pdf_path = pdf_paths[0]

breakpoint()

# '-f 2' to start processing on the second page of the PDF (since page 1 is a cover page).
pdftotext_process = subprocess.Popen([pdftotext, '-layout', pdf_path, '-f', '2', '-'], stdout=subprocess.PIPE)
tr_process = subprocess.Popen([tr, '-s', "' '"], stdin=pdftotext_process.stdout, stdout=subprocess.PIPE)
pdftotext_process.stdout.close()
out, err = tr_process.communicate()

print("All done")