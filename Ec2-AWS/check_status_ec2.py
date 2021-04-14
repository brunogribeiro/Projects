#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html?highlight=instances#instance
import boto3

class Checkec2():
    client=boto3.client('ec2')
    ec2 = boto3.resource('ec2')

    def check_id_ec2(self, tagname, state):
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                   {'Name': 'instance-state-name',
                                                    'Values': [state]}])
        list_id=[consulting.id for consulting in consulting]
        list_id.sort()
        return(list_id)

    def check_ip_public_ec2(self, tagname, state):
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                        {'Name': 'instance-state-name',
                                                         'Values': [state]}])

        list_ip=[consulting.public_ip_address for consulting in consulting]
        list_ip.sort()
        return(list_ip)


    def check_ip_private_ec2(self, tagname, state):
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                        {'Name': 'instance-state-name',
                                                         'Values': [state]}])

        list_ip=[consulting.private_ip_address for consulting in consulting]
        list_ip.sort()
        return(list_ip)

    def check_ebs_ec2_id(self,tagname):
        consulting = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [tagname]},
                                                        ])
        list_id = [consulting.id for consulting in consulting]
        for id_ec2 in list_id:
            instance = self.ec2.Instance(id_ec2)
            volumes = instance.volumes.all()
            for ebs in volumes:
                return(ebs.id)

# ip_ec2 = Checkec2()
# list_ec2 = ip_ec2.check_ip_ec2('Teste_ec2','running')
# print(list_ec2)