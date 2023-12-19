import bs4
import subprocess
from multiprocessing import Pool
#from concurrent.futures import ThreadPoolExecutor as Pool
import argparse

import asyncio, asyncssh, sys

parser = argparse.ArgumentParser(description='Run docker image on cloudlab machines from xml config of cluster')
parser.add_argument('--config_file', type=str, help='XML config containing description for cloudlab machine (Copy paste under Manifest when viewing the experiment)', default='config.xml')
parser.add_argument('--identity_file', type=str, help='SSH identity file to access cloudlab machine', default='~/cloudlab_nopass')
parser.add_argument('--username', type=str, help='Username', default='makneee')
parser.add_argument('--step', type=int, default=0)
args = parser.parse_args()

config_file = args.config_file
identity_file = args.identity_file

if __name__ == '__main__':
    with open(config_file, 'r') as f:
        xml = f.read()
        soup = bs4.BeautifulSoup(xml, 'lxml')
        
        async def run_on_node(i, conn, clients):
            is_client = i > 2
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

                await run_command(r'rm -rf replicated_kvstore && git clone https://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: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@github.com/y4xu/replicated_kvstore.git')
                await run_command(r'cd replicated_kvstore/scripts && ./run_1.sh')
            elif args.step == 1:
                base_dir = '~'
                await asyncssh.scp('mount.sh', (conn, f'{base_dir}/replicated_kvstore/scripts/mount.sh'))
                await run_command(f'cd {base_dir}/replicated_kvstore/scripts && ./mount.sh')
                await run_command(r'cp -r ~/replicated_kvstore /mnt/sda4/')
            elif args.step == 2:
                base_dir = '~'
                #await run_command(r'cp -r ~/replicated_kvstore /mnt/sda4/')
                #await asyncssh.scp('config_fasttransport', (conn, f'{base_dir}/replicated_kvstore/skyros/src/skyros/sample/config_fasttransport'))
                #await asyncssh.scp('config_fasttransport.client', (conn, f'{base_dir}/replicated_kvstore/skyros/src/skyros/sample/config_fasttransport.client'))

                #await asyncssh.scp('mount.sh', (conn, f'{base_dir}/replicated_kvstore/scripts/mount.sh'))
                #await asyncssh.scp('run-after-setup.sh', (conn, f'{base_dir}/replicated_kvstore/scripts/run-after-setup.sh'))

                await run_command(f'cd {base_dir}/replicated_kvstore && git checkout main && git pull')
                await run_command(f'cd {base_dir}/replicated_kvstore/scripts && (echo "n" | ./setup_eRPC.sh)')
                await run_command(f'cd {base_dir}/replicated_kvstore/scripts && ./folly.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/deps/ && sudo rm -rf eRPC && ./erpc.sh')
                await asyncssh.scp('splinter.sh', (conn, f'{base_dir}/replicated_kvstore/skyros/src/deps/splinter.sh'))
                await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/deps/ && sudo rm -rf splinterdb && ./splinter.sh')

                #await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/skyros/ && git checkout fasttransport_multiple_threads')
                await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/skyros/third_party && sudo rm -rf eRPC && ./setup.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/skyros && make clean && make -j')
                '''
                await run_command(f'cd {base_dir}/replicated_kvstore && git checkout main')
                await run_command(f'cd {base_dir}/replicated_kvstore/scripts && ./setup_eRPC.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore/scripts && ./folly.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/deps/ && ./erpc.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/deps/ && ./splinter.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/skyros && make clean && make -j')
                '''
            elif args.step == 3:
                base_dir = '/mnt/sda4'
                # base_dir = '~'

                if is_client:
                    await run_command(f'mkdir -p {base_dir}/traces')
                    await run_command(f'cd {base_dir}/traces && rm -f runt.tar.gz\?download\=1')
                    await run_command(f'cd {base_dir}/traces && wget https://zenodo.org/record/5520021/files/runt.tar.gz?download=1')
                    await run_command(f'cd {base_dir}/traces && mv runt.tar.gz\?download\=1 runt.tar')
                    await run_command(f'cd {base_dir}/traces && tar xvf runt.tar')
                    await run_command(f'cd {base_dir}/traces && rm -f runt.tar')
                else:
                    print('Skipping trace download')

            elif args.step == 4:
                base_dir = '~'

                await run_command(f'cd {base_dir}/replicated_kvstore && sudo chown -R {args.username} .')
                await run_command(f'cd {base_dir}/replicated_kvstore && git stash && git checkout perf && git pull')
                
                await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/deps/ && sudo rm -rf eRPC && ./erpc.sh')
                await asyncssh.scp('splinter.sh', (conn, f'{base_dir}/replicated_kvstore/protocols/src/deps/splinter.sh'))
                await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/deps/ && sudo rm -rf splinterdb && ./splinter.sh')

                await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia/third_party && sudo rm -rf eRPC && sudo rm -rf parallel-hashmap && ./setup.sh')

                await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && make clean && make -j')
            elif args.step == 5:
                base_dir = '~'

                await run_command(f'cd {base_dir}/replicated_kvstore && git stash && git fetch && git checkout perf-test')
                # await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia/third_party && sudo rm -rf eRPC && sudo rm -rf parallel-hashmap && ./setup.sh')
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia/third_party/ && rm -rf concurrentqueue && git clone https://github.com/cameron314/concurrentqueue')
                # await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && make clean && make -j')
                # await run_command(f'sudo bash -c "echo 16192 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')

                await asyncssh.scp('config_fasttransport', (conn, f'{base_dir}/replicated_kvstore/protocols/src/ionia/config_fasttransport'))
                await asyncssh.scp('config_fasttransport.client', (conn, f'{base_dir}/replicated_kvstore/protocols/src/ionia/config_fasttransport.client'))

                await run_command(f'cd {base_dir} && rm -f protobuf-cpp-3.21.12.tar.gz && wget https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protobuf-cpp-3.21.12.tar.gz')
                await run_command(f'cd {base_dir} && tar xvf protobuf-cpp-3.21.12.tar.gz && rm -f protobuf-cpp-3.21.12.tar.gz')
                await run_command(f'cd {base_dir} && cd protobuf-3.21.12 && ./autogen.sh && ./configure && make -j && sudo make install && sudo ldconfig')

                await run_command('git clone https://github.com/jemalloc/jemalloc.git && cd jemalloc && autoconf && ./configure && make dist && sudo make install')

                await asyncssh.scp('vr.py', (conn, f'{base_dir}/replicated_kvstore/protocols/experiments/vr.py'))
                await run_command(f'wget https://github.com/rui314/mold/releases/download/v1.10.1/mold-1.10.1-x86_64-linux.tar.gz')
                await run_command(f'tar xvf mold-1.10.1-x86_64-linux.tar.gz')
                await run_command(f'cd ~/mold-1.10.1-x86_64-linux/ && sudo cp -r bin/* /usr/local/bin && sudo cp -r lib/* /usr/local/lib')

                await run_command(f'sudo apt install cpufrequtils -y && echo \'GOVERNOR="userspace"\' | sudo tee /etc/default/cpufrequtils && sudo systemctl restart cpufrequtils')
                await run_command(f'sudo apt install linux-tools-common -y && sudo apt install linux-tools-5.4.0-100-generic -y && sudo cpupower frequency-set -g performance && sudo cpupower frequency-set -u 4.0GHz && sudo cpupower frequency-set -d 4.0GHz')

                await asyncssh.scp('cloudlab_nopass', (conn, f'cloudlab_nopass'))
                await run_command(f'sudo apt install ccache htop sysstat -y')
                await run_command(f'curl -L https://bit.ly/glances | /bin/bash')
            elif args.step == 6:
                base_dir = '~'
                traces_dir = '/mnt/sda4'
                
                #await run_command(f'chmod +x {base_dir}/replicated_kvstore/protocols/experiments/udp_parms')
                #await run_command(f'sudo {base_dir}/replicated_kvstore/protocols/experiments/udp_parms')
                
                # await run_command(f'sudo apt install ccache -y')
                # await run_command(f'sudo mkdir -p /ramdisk/ccache && sudo mount -t tmpfs -o size=2048M tmpfs /ramdisk/ccache && sudo chown -R {args.username} /ramdisk/ccache')
                # await run_command(f'echo "cache_dir = /ramdisk/ccache" > {base_dir}/.ccache/ccache.conf')
                # await run_command(f'sudo rm -rf /tmp && sudo rm -rf {base_dir}/.cache && sudo rm -rf {base_dir}/.vscode-server && sudo rm -rf {base_dir}/jemalloc')
                # await run_command(r'sudo rm -rf /tmp && sudo find ~/replicated_kvstore -size +20M -delete')
                # await run_command(f'sudo rm -rf {base_dir}/traces/runt')
                # await run_command(f'sudo rm -rf {base_dir}/replicated_kvstore/protocols/src/ionia/third_party/eRPC/third_party')
                # await run_command(f'sudo rm -rf {base_dir}/replicated_kvstore/protocols/src/ionia/third_party/eRPC/lib')
                await asyncssh.scp('config_fasttransport', (conn, f'{base_dir}/replicated_kvstore/protocols/src/ionia/config_fasttransport'))
                await asyncssh.scp('config_fasttransport.client', (conn, f'{base_dir}/replicated_kvstore/protocols/src/ionia/config_fasttransport.client'))
                #await run_command(f'cd {base_dir}/replicated_kvstore && sudo pkill replica')

                # if is_client:
                #     await asyncssh.scp('split_results.sh', (conn, f'{traces_dir}/traces/runt/split_results.sh'))
                #     await run_command(f'cd {traces_dir}/traces/runt && ./split_results.sh')

                if is_client:
                    if i == 3:
                        for RUN in ['75_25', '25_75', '0_100']:
                            # RUN = 'c'
                            RUN_FILENAME = f'run{RUN}'
                            CLIENTS = 15
                            await run_command(f'cd {traces_dir} && rm -rf ycsb-tracegen')
                            await run_command(f'cd {traces_dir} && git clone https://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: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@github.com/dassl-uiuc/ycsb-tracegen.git')
                            await run_command(f'cd {traces_dir}/ycsb-tracegen && sudo apt install maven -y')
                            await run_command(f'cd {traces_dir}/ycsb-tracegen && mvn clean package -T 1C -DskipTests || true')
                            await run_command(f'cd {traces_dir}/ycsb-tracegen && ./gen-trace.py load && ./convert.py 32 load')
                            
                            await run_command(f'cd {traces_dir}/ycsb-tracegen && ./gen-trace.py run {RUN} && ./convert.py 80 {RUN}')
                            await run_command(f'cd {traces_dir} && mkdir -p traces && cd {traces_dir}/traces/ && rm -rf {RUN_FILENAME}')
                            await run_command(f'cd {traces_dir}/ycsb-tracegen && cp -rf client_traces/run* {traces_dir}/traces')
                            ## await run_command(f'cd {traces_dir}/ycsb-tracegen && cp -rf client_traces/load {traces_dir}/traces/{RUN_FILENAME}')
                            aa()

                            ## await asyncssh.scp('split_results_load.sh', (conn, f'{traces_dir}/traces/{RUN_FILENAME}/load/split_results_load.sh'))
                            ## await run_command(f'cd {traces_dir}/traces/{RUN_FILENAME}/load && ./split_results_load.sh w {4 * CLIENTS}')
                            ## await asyncssh.scp('split_results.sh', (conn, f'{traces_dir}/traces/{RUN_FILENAME}/split_results.sh'))
                            ## await run_command(f'cd {traces_dir}/traces/{RUN_FILENAME} && ./split_results.sh {RUN} {4 * 15}')

                        commands = []
                        for j in range(4, len(clients)):
                            username, hostname = clients[j]
                            c = run_command(f'cd {traces_dir} && rsync -avP -e "ssh -oStrictHostKeyChecking=no -i {args.identity_file}" traces {username}@{hostname}:{traces_dir}')
                            commands.append(c)
                        [await c for c in commands]
                        await asyncio.gather(*commands)

                        await run_command(f'cd {traces_dir}/ && true')
                    await run_command(f'cd {traces_dir}/ && true')
                aa()

                await run_command(f'sudo pkill -9 replica || true')
                await run_command(f'sudo pkill -9 client || true')
                await asyncssh.scp('io_stat.sh', (conn, f'/tmp/io_stat.sh'))
                await run_command(f'chmod +x /tmp/io_stat.sh')
                await asyncssh.scp('glances.sh', (conn, f'/tmp/glances.sh'))
                await run_command(f'chmod +x /tmp/glances.sh')

                # if i >= 3:
                #     # await run_command(f'rm -rf {traces_dir}/traces/rund')
                #     username = 'makneee'
                #     hostname = f'node{4 + (i % 3)}.SkyrosPPDev16.rasl-PG0.utah.cloudlab.us'
                #     await run_command(f'cd {traces_dir} && rsync -avP -e "ssh -oStrictHostKeyChecking=no -i {args.identity_file}" {username}@{hostname}:{traces_dir}/traces /mnt/sda4')

                # await run_command(f'cd {base_dir}/ && rm -rf protobuf-3.21.12 && rm -rf jemalloc || true')
                # await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && sudo rm -rf third_party/protobuf-3.21.12 || true')
                # await run_command(f'cd {base_dir}/replicated_kvstore/skyros/src/deps && sudo rm -rf eRPC || true')
                # await run_command(f'cd {base_dir}/replicated_kvstore/scripts && sudo rm -rf MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64.iso || true')
                # await run_command(f'cd {base_dir}/ && rm -rf .ccache || true')

                # if is_client:
                #     await run_command(f'cd {traces_dir}/traces/load && rm -rf all_files.txt')
                #     await asyncssh.scp('split_results_load.sh', (conn, f'{traces_dir}/traces/load/split_results_load.sh'))
                #     await run_command(f'cd {traces_dir}/traces/load && ./split_results_load.sh w {4 * 15}')

                if not is_client:
                    await asyncssh.scp('format_disk.sh', (conn, f'{base_dir}/format_disk.sh'))
                    await run_command(f'echo "y" | sudo {base_dir}/format_disk.sh')
                    # await run_command(f'sudo bash -c "echo 16384 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')
                    await run_command(f'sudo bash -c "echo 12384 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')
                    # await run_command(f'sudo bash -c "echo 8192 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')

                    # await asyncssh.scp('download_and_unpack_splinter.sh', (conn, f'{base_dir}/download_and_unpack_splinter.sh'))
                    # await run_command(f'chmod +x {base_dir}/download_and_unpack_splinter.sh')
                    # await run_command(f'cd {base_dir}/ && {base_dir}/download_and_unpack_splinter.sh makneee@10.10.1.16')
                else:
                    await run_command(f'sudo bash -c "echo 8192 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')

                # # await run_command(f'curl -LO https://github.com/ClementTsang/bottom/releases/download/0.8.0/bottom_0.8.0_amd64.deb && sudo dpkg -i bottom_0.8.0_amd64.deb')
                # # double check if cleared db, should fail
                # await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && sudo rm /mnt/sda4/bench/replica')

                if not is_client:
                    await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && sudo rm -rf /mnt/sda4/splinterdb.db')
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/deps/ && sudo rm -rf splinterdb && ./splinter.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore && git stash && pkill python && sudo pkill client || true')
                #await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-foreground-change-ycsb-c-splinter && git pull')
                #await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-unreplicated && git pull && rm -rf /tmp/ionia_lock')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads && git pull && rm -rf /tmp/ionia_lock')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-full-applied-index-step-2 && git pull')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-test-sync-reads-false && git pull')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-test-sync-reads-false-old-mapping && git pull')
                # await run_command(f'sudo bash -c "echo 12000 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')
                # await run_command(f'sudo bash -c "echo 16384 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')
                # await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia/third_party && ./setup.sh')


                await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-full && git pull && rm -rf /tmp/ionia_lock')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-full-throughput-second-client && git pull && rm -rf /tmp/ionia_lock')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-full-applied-index-step-2 && git pull')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-pe && git pull')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-unreplicated && git pull && rm -rf /tmp/ionia_lock')

                # await run_command(f'cd {base_dir}/replicated_kvstore && git fetch && git checkout perf-test-execution-reads-pe && git pull')

                # await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && mold -run make -j')
                await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && make clean && mold -run make -j')

                # await run_command(f'cd {base_dir} && rm -rf splinter-odirect-stat && git clone https://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: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@github.com/y4xu/splinter-odirect-stat/')
                # await run_command(f'cd {base_dir}/splinter-odirect-stat/splinter-ycsb/YCSB-C && sudo apt-get install libtbb-dev librocksdb-dev libhiredis-dev -y && make && cp ycsbc /mnt/sda4/ycsbc && cp -r workloads /mnt/sda4/')
                # /mnt/sda4/ycsbc -db splinterdb -threads 12 -L workloads/load.spec -w fieldlength 100 -w recordcount 670000000 -W workloads/workloada.spec -w operationcount 160000000

                #await run_command(f'cd {base_dir}/replicated_kvstore && git stash && git fetch && git checkout perf-paxos-fasttransport')
                #await run_command(f'sudo bash -c "echo 24192 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"')
                #await asyncssh.scp('vrclient.py', (conn, f'{base_dir}/replicated_kvstore/protocols/experiments/vrclient.py'))
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/paxos && git stash && git pull && make clean && make -j')
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia/third_party && sudo rm -rf eRPC && sudo rm -rf parallel-hashmap && ./setup.sh')
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/paxos && git stash && git pull && make -j')

                #await run_command(f'cd {base_dir}/replicated_kvstore && git stash && git fetch && git checkout perf-skyros-fasttransport')
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/skyros && make clean && make -j')
                
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/deps/ && sudo rm -rf eRPC && ./erpc.sh')
                #await asyncssh.scp('splinter.sh', (conn, f'{base_dir}/replicated_kvstore/protocols/src/deps/splinter.sh'))
                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/deps/ && sudo rm -rf splinterdb && ./splinter.sh')

                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia/third_party && sudo rm -rf eRPC && ./setup.sh')

                #await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && make clean && make -j')
            elif args.step == 7:
                base_dir = '/mnt/sda4'

                await run_command(f'cd {base_dir} && rm -f protobuf-cpp-3.21.12.tar.gz && wget https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protobuf-cpp-3.21.12.tar.gz')
                await run_command(f'cd {base_dir} && tar xvf protobuf-cpp-3.21.12.tar.gz && rm -f protobuf-cpp-3.21.12.tar.gz')
                await run_command(f'cd {base_dir} && cd protobuf-3.21.12 && ./autogen.sh && ./configure && make -j && sudo make install && sudo ldconfig')
            elif args.step == 8:
                base_dir = '~'
                await asyncssh.scp('format_disk.sh', (conn, f'{base_dir}/format_disk.sh'))
                await asyncssh.scp('splinter.sh', (conn, f'{base_dir}/replicated_kvstore/protocols/src/deps/splinter.sh'))
                await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/deps/ && sudo rm -rf splinterdb && ./splinter.sh')
                await run_command(f'cd {base_dir}/replicated_kvstore/protocols/src/ionia && make clean && mold -run make -j')

                # await run_command(f'sudo apt install zstd pzstd -y')
                # await run_command(f'cd {base_dir}/replicated_kvstore && git stash && git fetch && git checkout perf-test-execution-foreground-change')


            return True
        
        interfaces = list(soup.find_all('login'))
        def get_ssh_endpoint(interface):
            ssh_endpoint = f'{interface["username"]}@{interface["hostname"]}'
            return ssh_endpoint
        ssh_endpoints = [get_ssh_endpoint(interface) for interface in interfaces if args.username in interface["username"]]
        def get_username_hostname(interface):
            username = interface['username']
            hostname = interface['hostname']
            if args.username != username:
                return None
            return username, hostname
        username_hostname = [get_username_hostname(interface) for interface in interfaces]
        cloudlab_password = ''
        p_key = asyncssh.read_private_key(identity_file, cloudlab_password)

        async def run_client(i, username_hostname, clients) -> asyncssh.SSHCompletedProcess:
            username = username_hostname[0]
            hostname = username_hostname[1]
            async with asyncssh.connect(host=hostname, username=username, port=22, client_keys=[p_key], known_hosts=None) as conn:
                r = await run_on_node(i, conn, clients)
                return r 

        async def run_multiple_clients() -> None:
            # Put your lists of hosts here
            clients = [u for u in username_hostname if u is not None]
            tasks = (run_client(i, c, clients) for i, c in enumerate(clients))
            #tasks = (run_client(u) for u in username_hostname[:5])
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results, 1):
                if isinstance(result, Exception):
                    print('Task %d failed: %s' % (i, str(result)))
                else:
                    print(f'Task {i} Success')
        
        asyncio.run(run_multiple_clients())

