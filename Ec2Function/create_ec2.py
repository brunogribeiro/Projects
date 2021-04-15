import boto3
import check_status_ec2
import time
#import configuracao as conf

class CreateEc2():
	"""
		Class used to create EC2 resources on AWS
    """
	ec2 = boto3.resource('ec2')
    client = boto3.client('ec2')
    
	def CreateInstance(self,imageid,InstanceType,tagname):
		"""
		Create EC2 resources in AWS
		args:
			imagineid(str): ID AMI in aws
			Instacetyoe(str): Type of instance for EC2 resources on AWS
			tagname(str): Name of EC2 resources on AWS. 
		return:
			Returns after positive status confirmations the parameters of Private IP and Name of the created EC2 resource
		"""		
		id_ec2 = check_status_ec2.Checkec2()
        list_ec2 = id_ec2.check_id_ec2(tagname, 'running')
        while (len(list_ec2) == 1):
            print(tagname + " machines are already created!!!")
            break
        else:
            self.ec2.create_instances(ImageId=imageid,
                                 MinCount=1,
                                 MaxCount=1,
                                 KeyName='Key.PEM',
                                 InstanceType=InstanceType,
                                 TagSpecifications=[{'ResourceType': 'instance',
                                                     'Tags': [{'Key': 'Name',
                                                               'Value': tagname,
                                                     }]
                                                   }],
                                 NetworkInterfaces=[{'AssociatePublicIpAddress': True,
                                                     'DeleteOnTermination': True,
                                                     'DeviceIndex': 0,
                                                     'SubnetId': 'subnet-XXXXXXXXXX',
                                                     'Groups': ['sg-XXXXXXXXXXXXXXX'],
                                                     }]
                                 )
            print('Starting the creation of EC2')
            time.sleep(50)
            print('Fetching EC2 ID')
            id_ec2 = check_status_ec2.Checkec2()
            list_ip=id_ec2.check_ip_private_ec2(tagname, 'running')
            list_id=id_ec2.check_id_ec2(tagname, 'running')
            print('EC2 IDs found:', [list_id])
            print('Waiting for OK status on AWS on EC2', tagname, [list_id])
            waiter = self.client.get_waiter('instance_status_ok')
            waiter.wait(InstanceIds=list_id)
            print('Status OK in AWS !!')
            print('Add tag ', tagname,'in EBS')
            id_ebs = id_ec2.check_ebs_ec2_id(tagname)
            self.ec2.create_tags(Resources=[id_ebs], Tags= [{'Key': 'Name','Value': tagname}])
            print('Tagname add in EBS')
            print('EC2 created with name', tagname ,'and id:', list_id, 'and IP', list_ip ,' is ready to use!')

"""
HOW TO USE FUNCITION
CreateEc2().CreateInstance('ami-0a2ea2e3c8eef83a9',conf.aws['ec2typeMed'],'Airflow')
""""



