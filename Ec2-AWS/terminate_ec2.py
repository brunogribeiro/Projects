import boto3
import check_status_ec2
import time

class TerminateEc2():
    ec2 = boto3.resource('ec2')
    client = boto3.client('ec2')

    def Terminate(self,tagname,state):
        id_ec2 = check_status_ec2.Checkec2()
        list_ec2 = id_ec2.check_id_ec2(tagname,state)
        list_ebs = id_ec2.check_ebs_ec2_id(tagname)
        print(list_ebs)
        print(list_ec2)
        print(f'TAG: {tagname}')
        for id in list_ec2:
            id_ec2 = id
            self.ec2.instances.filter(InstanceIds=[id_ec2]).terminate()
            print(id_ec2 + ' Finalizada com sucesso!')
        time.sleep(60)
        if list_ebs is not None:
            try:
                self.client.delete_volume(VolumeId=list_ebs)
            except:
                pass
            else:
                print("EBS foi removido!")

# ip_ec2 = TerminateEc2()
# list_ec2 = ip_ec2.Terminate('Teste_ec2','running')
