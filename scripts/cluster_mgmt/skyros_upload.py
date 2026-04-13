import bs4
import subprocess
from multiprocessing import Pool
#from concurrent.futures import ThreadPoolExecutor as Pool
import argparse

import asyncio, asyncssh, sys

parser = argparse.ArgumentParser(description='Run docker image on cloudlab machines from xml config of cluster')
parser.add_argument('--config_file', type=str, help='XML config containing description for cloudlab machine (Copy paste under Manifest when viewing the experiment)', default='config.xml')
parser.add_argument('--identity_file', type=str, help='SSH identity file to access cloudlab machine', default='cloudlab.pem')
parser.add_argument('--username', type=str, help='Username', default='makneee')
parser.add_argument('--step', type=int, default=0)
parser.add_argument('--repo_url', type=str, default='https://github.com/ntu2323/IONIA.git')
parser.add_argument('--repo_dir', type=str, default='IONIA')
parser.add_argument('--branch', type=str, default='main')
args = parser.parse_args()

config_file = args.config_file
identity_file = args.identity_file

if __name__ == '__main__':
    with open(config_file, 'r') as f:
        xml = f.read()
        soup = bs4.BeautifulSoup(xml, 'lxml')
        
        async def run_on_node(conn):
            async def run_command(command):
                print(f'[{conn._host}] Running {command}...')
                try:
                    result = await conn.run(command, check=True)
                except Exception as e:
                    raise Exception(f'Failed running {command} with exception: {e}')

                print(f'[{conn._host}] Finished running {command}...')
                return result.stdout

            if args.step == 0:
                stdout = await run_command(r'sudo apt update -y')
                await run_command(r'sudo apt-get install libboost-all-dev libdouble-conversion-dev libgoogle-glog-dev build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc -y')
                #await run_command(r'wget https://github.com/rui314/mold/releases/download/v1.1.1/mold-1.1.1-x86_64-linux.tar.gz')
                #await run_command(r'tar xvf mold-1.1.1-x86_64-linux.tar.gz')
                #await run_command(r'sudo cp -r mold-1.1.1-x86_64-linux/* /usr')
                #await run_command(r'rm -rf mold-1.1.1-x86_64-linux/')

                await run_command(
                    f'rm -rf {args.repo_dir} && git clone {args.repo_url} {args.repo_dir}'
                )
                await run_command(f'cd {args.repo_dir}/scripts && ./run_1.sh')
            elif args.step == 1:
                base_dir = '~'
                await asyncssh.scp('mount.sh', (conn, f'{base_dir}/{args.repo_dir}/scripts/mount.sh'))
                await run_command(f'cd {base_dir}/{args.repo_dir}/scripts && ./mount.sh')
                await run_command(f'cp -r ~/{args.repo_dir} /mnt/sda4/')
            elif args.step == 2:
                base_dir = '/mnt/sda4'

                await run_command(
                    f'cd {base_dir}/{args.repo_dir} && git fetch origin && git checkout {args.branch} && git pull'
                )
                await run_command(f'cd {base_dir}/{args.repo_dir}/scripts && (echo "n" | ./setup_eRPC.sh)')
                await run_command(f'cd {base_dir}/{args.repo_dir}/scripts && ./folly.sh')
                await run_command(
                    f'cd {base_dir}/{args.repo_dir}/protocols/src/deps/ && sudo rm -rf eRPC && ./erpc.sh'
                )
                await run_command(
                    f'cd {base_dir}/{args.repo_dir}/protocols/src/deps/ && sudo rm -rf splinterdb && ./splinter.sh'
                )
                await run_command(
                    f'cd {base_dir}/{args.repo_dir}/protocols/src/ionia/third_party && '
                    f'sudo rm -rf eRPC && sudo rm -rf parallel-hashmap && ./setup.sh'
                )
                await run_command(
                    f'cd {base_dir}/{args.repo_dir}/protocols/src/ionia && make clean && make -j'
                )
                
            return True
        
        interfaces = list(soup.find_all('login'))
        def get_ssh_endpoint(interface):
            ssh_endpoint = f'{interface["username"]}@{interface["hostname"]}'
            return ssh_endpoint
        ssh_endpoints = [get_ssh_endpoint(interface) for interface in interfaces if args.username in interface["username"]]
        def get_username_hostname(interface):
            username = interface['username']
            hostname = interface['hostname']
            return username, hostname
        username_hostname = [get_username_hostname(interface) for interface in interfaces]
        cloudlab_password = ''
        p_key = asyncssh.read_private_key(identity_file, cloudlab_password)

        async def run_client(username_hostname) -> asyncssh.SSHCompletedProcess:
            username = username_hostname[0]
            hostname = username_hostname[1]
            async with asyncssh.connect(host=hostname, username=username, port=22, client_keys=[p_key], known_hosts=None) as conn:
                r = await run_on_node(conn)
                return r 

        async def run_multiple_clients() -> None:
            # Put your lists of hosts here
            tasks = (run_client(u) for u in username_hostname)
            #tasks = (run_client(u) for u in username_hostname[:5])
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results, 1):
                if isinstance(result, Exception):
                    print('Task %d failed: %s' % (i, str(result)))
                else:
                    print(f'Task {i} Success')
        
        asyncio.run(run_multiple_clients())

