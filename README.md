# ccdict
A command-line based utility for looking up CC-Canto (Cantonese) dictionary data

## Setup notes
### [Adapted python virtualenv setup](https://realpython.com/intro-to-pyenv/#installing-pyenv)
```shell
# pyenv dependencies
sudo apt-get update
sudo apt-get install -y make  \
                        build-essential \
sudo apt-get install -y libssl-dev \
                        zlib1g-dev \
                        libbz2-dev \
                        libreadline-dev \
                        libsqlite3-dev \
                        wget \
                        curl \
                        llvm \
                        libncurses5-dev \
                        libncursesw5-dev \
                        xz-utils \
                        tk-dev \
                        libffi-dev \
                        liblzma-dev

# pyenv installer
curl https://pyenv.run | bash

# Install python version with loadable sqlite extensions
PYTHON_VERSION=3.12.7
PYTHON_CONFIGURE_OPTS="--enable-loadable-sqlite-extensions" pyenv install $PYTHON_VERSION
```

### `sqlean` (`sqlite3` extensions)
Unpack the [sqlean libraries](https://github.com/nalgeon/sqlean/blob/main/docs/install.md#download-manually)
to a location accessible to the ccdict code, then load required extensions via
`sqlite3` Connection object.
```python
import sqlite3

db_con = sqlite3.connect(":memory:")
db_con.load_extension("/path/to/sqlite3_extensions/regexp")
```

### `click` and `click-shell`
```shell
pip install click

# Install click-shell from source to access features not available in wheels
export CLICK_VERSION=8.0.1
git clone https://github.com/clarkperkins/click-shell.git
cd /click-shell
CLICK_VERSION=8.0.1 ./install.sh
```
