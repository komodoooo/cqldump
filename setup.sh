#!/bin/sh
pip3 install -r requirements.txt
chmod +x cqldump.py
sudo cp cqldump.py /bin/cqldump
echo "setup done, ready to run cqldump"
