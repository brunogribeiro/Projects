#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html?highlight=instances#instance
import boto3

class Checkec2():
    client=boto3.client('ec2')
    ec2 = boto3.resource('ec2')

    def check_id_ec2(self, tagname, state):
		"""
		Check ID EC2 in AWS
		args:
			tagname(str): ec2 resource tagname in aws
			state(str): AWS resource state. 
		return:
			(lst) Returns list of ID's of ec2 resources in aws according to state parameter
		"""
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                   {'Name': 'instance-state-name',
                                                    'Values': [state]}])
        list_id=[consulting.id for consulting in consulting]
        list_id.sort()
        return(list_id)

    def check_ip_public_ec2(self, tagname, state):
		"""
		Check Ip PUBLIC in AWS
		args:
			tagname(str): ec2 resource tagname in aws
			state(str): AWS resource state. 
		return:
			(lst) Returns list of IP PUBLIC of ec2 resources in aws according to state parameter
		"""
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                        {'Name': 'instance-state-name',
                                                         'Values': [state]}])

        list_ip=[consulting.public_ip_address for consulting in consulting]
        list_ip.sort()
        return(list_ip)


    def check_ip_private_ec2(self, tagname, state):
		"""
		Check IP PRIVATE in AWS
		args:
			tagname(str): ec2 resource tagname in aws
			state(str): AWS resource state. 
		return:
			(lst) Returns list of IP PRIVATE of ec2 resources in aws according to state parameter
		"""
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                        {'Name': 'instance-state-name',
                                                         'Values': [state]}])

        list_ip=[consulting.private_ip_address for consulting in consulting]
        list_ip.sort()
        return(list_ip)

    def check_ebs_ec2_id(self,tagname):
		"""
		Check EBS ID in AWS
		args:
			tagname(str): ec2 resource tagname in aws
			state(str): AWS resource state. 
		return:
			(lst) Returns list of EBS ID of ec2 resources in aws according to state parameter
		"""
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                        ])
        list_id = [consulting.id for consulting in consulting]
        for id_ec2 in list_id:
            instance = self.ec2.Instance(id_ec2)
            volumes = instance.volumes.all()
            for ebs in volumes:
                return(ebs.id)

"""
HOW TO EXECUTE FUNCTION:

ip_ec2 = Checkec2()
list_ec2 = ip_ec2.check_ip_ec2('Teste_ec2','running')
print(list_ec2)

"""