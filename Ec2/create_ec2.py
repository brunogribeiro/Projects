import boto3
import check_status_ec2
import time
#import configuracao as conf
class CreateEc2():
    ec2 = boto3.resource('ec2')
    client = boto3.client('ec2')
    def CreateInstance(self,imageid,InstanceType,tagname):
        id_ec2 = check_status_ec2.Checkec2()
        list_ec2 = id_ec2.check_id_ec2(tagname, 'running')
        while (len(list_ec2) == 1):
            print(tagname + " machines are already created!!!")
            break
        else:
            self.ec2.create_instances(ImageId=imageid,
                                 MinCount=1,
                                 MaxCount=1,
                                 KeyName='chaveEC2_biASQ',
                                 InstanceType=InstanceType,
                                 TagSpecifications=[{'ResourceType': 'instance',
                                                     'Tags': [{'Key': 'Name',
                                                               'Value': tagname,
                                                     }]
                                                   }],
                                 NetworkInterfaces=[{'AssociatePublicIpAddress': True,
                                                     'DeleteOnTermination': True,
                                                     'DeviceIndex': 0,
                                                     #subnet publica: subnet-05aefdf42feb606a7
                                                     'SubnetId': 'subnet-05aefdf42feb606a7',
                                                     #subnet privada: subnet-087dd34478cf3f991
                                                     # 'SubnetId': 'subnet-087dd34478cf3f991',
                                                     # sg privado: sg-0ae5bd11857234940
                                                     # 'Groups': ['sg-0ae5bd11857234940'],
                                                     # sg publico: sg-00248f7d85ddb3f54
                                                     'Groups': ['sg-00248f7d85ddb3f54'],
                                                     }]
                                 )
            print('Iniciando criação da EC2')
            time.sleep(50)
            print('Buscando ID EC2')
            id_ec2 = check_status_ec2.Checkec2()
            list_ip=id_ec2.check_ip_private_ec2(tagname, 'running')
            list_id=id_ec2.check_id_ec2(tagname, 'running')
            print('IDs EC2 encontradas:', [list_id])
            print('Aguardando status OK na AWS na EC2:', tagname, [list_id])
            waiter = self.client.get_waiter('instance_status_ok')
            waiter.wait(InstanceIds=list_id)
            print('Status OK na AWS !!')
            print('Adicionando a tag ', tagname,'no EBS')
            id_ebs = id_ec2.check_ebs_ec2_id(tagname)
            self.ec2.create_tags(Resources=[id_ebs], Tags= [{'Key': 'Name','Value': tagname}])
            print('Tagname Adiconada ao EBS')
            print('A máquina ', tagname ,'de id:', list_id, 'e IP', list_ip ,' está pronta para uso!')
            print('Até mais ! =) ')

#CreateEc2().CreateInstance('ami-0a2ea2e3c8eef83a9',conf.aws['ec2typeMed'],'Airflow')



