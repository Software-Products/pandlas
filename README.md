#  Pandlas

A powerful Python package that extends Pandas functionality to work with ATLAS SQLRace API, providing seamless data analysis and manipulation capabilities

[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)


This package utilises API from ATLAS and as such requires a valid ATLAS licence with the SQLRace option included.

## Installtion
```
pip install "git+https://github.com/Software-Products/pandlas.git"
```

## Package dependencies
- Pandas
- pythonnet
- tqdm

# Limitations
- Only take a dataframe of float or can be converted to float
- Must have a DateTime index

Further possibilities with SQLRace API but not implemented in this Python package
- Text Channels
- Set custom limits and warnings in ATLAS
- Grouping parameters
