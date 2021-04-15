import asyncio, asyncssh, sys
import check_status_ec2

key='KEY.pem'

class exec():

    async def run_client(host, command):
		"""
		Assync function for ssh connection and executing a bash command
		args:
			host(str): Host for EC2 resource in AWS
			command(str): Command to be executed via the ssh connection 
		return:
			If successful execution returns the stout, if it fails, return the stout and sterr	
		"""	
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
		"""
		Function that checks if the EC2 resource is available and checks Ec2's private ip. Use this data to call the run_client function.
		args:
			tagname(str): Name of EC2 resources on AWS.
			command(str): Command to be executed via the ssh connection 
		return:
			Returns availability information for the Ec2 resource or error the ssh connection
		"""
        ip_ec2 = check_status_ec2.Checkec2()
        host = ip_ec2.check_ip_private_ec2(tagname,'running')
        if host == []:
            sys.exit('No VM was found with the tagname: ' + tagname)
        else:
            for h in host:
                print('Conectando no host:' + h)
                print('Execute command: ' + command)
                try:
                    asyncio.get_event_loop().run_until_complete(exec.run_client(h, command))
                except (OSError, asyncssh.Error) as exc:
                    sys.exit('Conexion SSH failed =[ : ' + str(exc))

"""
HOW TO EXECUTE FUNCTION:
exec().run_command('tagname','date')
""""