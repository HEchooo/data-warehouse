#!/usr/bin/python3
"""堡垒机连接入口。私钥随文件夹提供（env/id_rsa），不依赖个人 ~/.ssh 配置。"""
import subprocess
from pathlib import Path

SSH_KEY = Path(__file__).resolve().parent / "id_rsa"
BASTION = "huxin@jump.valleysound.xyz"
PORT = "2222"


def main():
    command = ["ssh", "-i", str(SSH_KEY), "-p", PORT, BASTION]
    print("\n可用命令列表：")
    print("[1] 电商：gcp堡垒机（%s:%s）" % (BASTION, PORT))
    try:
        number = input("\n输入要执行的命令编号：").removesuffix('\n')
    except KeyboardInterrupt:
        print('Cancled.')
        return
    if number != '1':
        return
    try:
        action = input('\n确认执行 %s:%s？[Y/n]：' % (BASTION, PORT)).removesuffix('\n')
    except KeyboardInterrupt:
        print('Cancled.')
        return
    if action == 'Y' or action == 'y' or action == '':
        subprocess.run(command)


main()
