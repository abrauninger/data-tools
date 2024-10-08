# data-tools
Scripts and tools for ingesting data.

## P223 data

CSV of P223 data published by Seattle Public Schools from September 2019 through September 2024:

[output/p223/all.csv](/output/p223/all.csv)

### Updating P223 data

#### Setup

**Note:** This has only been tested on macOS.

```console
$ brew install pdftotext
```

`tr` is also required, but `tr` should already be installed by the OS:

```console
$ which tr
/usr/bin/tr
```

#### Extracting data from all P223 PDFs
P223 PDFs from Seattle Public Schools are committed to the [input/p223](/input/p223) directory in this repo.

To extract data from all PDFs in [input/p223](/input/p223) to [output/p223/all.csv](/output/p223/all.csv), run:

```console
$ python3 extractors/p223_pdf_batch.py
```

#### Extracting data from a single P223 PDF
```console
$ curl \
    https://www.seattleschools.org/wp-content/uploads/2024/09/P223_Sep24.pdf \
    -o p223_sep24.pdf
$ pdftotext -layout p223_sep24.pdf -f 2 - | tr -s ' ' > squished.txt
$ python3 extractors/p223_pdf_to_csv.py squished.txt out.csv
```

