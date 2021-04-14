import asyncio, asyncssh, sys
import check_status_ec2

key='KEY.pem'

class exec():

    async def run_client(host, command):
        async with asyncssh.connect(host,username='ubuntu', client_keys=[key],known_hosts=None) as conn:
            print('Conexão realizada com sucesso! =] ')
            result = await conn.run(command)
            if result.exit_status == 0:
                print(result.stdout, end='')
            else:
                print(result.stderr, end='', file=sys.stderr)
                #print('Program exited with status ' result.exit_status)
                print(result.stdout)
                print(result.stderr)
                sys.exit('SSH connection failed')


    def run_command(self, tagname,command):
        ip_ec2 = check_status_ec2.Checkec2()
        host = ip_ec2.check_ip_private_ec2(tagname,'running')
        if host == []:
            sys.exit('Não foi encontrada nenhuma VM com a tag: ' + tagname)
        else:
            for h in host:
                print('Conectando no host: ' + h)
                print('Executando o comando: ' + command)
                try:
                    asyncio.get_event_loop().run_until_complete(exec.run_client(h, command))
                except (OSError, asyncssh.Error) as exc:
                    sys.exit('Conexão SSH falhou =[ : ' + str(exc))
