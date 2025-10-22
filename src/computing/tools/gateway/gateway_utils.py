import paramiko
from src.common.config import get_config

def build_gateway_iptable(*args):

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        GATEWAY_NODE = get_config("computing", "gateway_node")
        private_key = paramiko.RSAKey.from_private_key_file("/root/.ssh/id_rsa")
        client.connect(f"{GATEWAY_NODE}", port=22, username="root", pkey=private_key)

        args_string = ' '.join(args)
        command = f'bash /root/INK/sshd/connect.sh {args_string}'

        stdin, stdout, stderr = client.exec_command(command)

        output = stdout.read().decode()
        error = stderr.read().decode()

        if error:
            raise Exception(f"Build gateway iptable failed, and details: {error}")

        return output
    
    except Exception as e:
        raise e
    
    finally:
        client.close()
    

def delete_gateway_iptable(port):

    GATEWAY_NODE = get_config("computing", "gateway_node")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        private_key = paramiko.RSAKey.from_private_key_file("/root/.ssh/id_rsa")
        client.connect(f"{GATEWAY_NODE}", port=22, username="root", pkey=private_key)
    
        command = f'bash /root/INK/sshd/delete_iptables.sh {port}'
        stdin, stdout, stderr = client.exec_command(command)

        output = stdout.read().decode()
        error = stderr.read().decode()

        if error:
            raise Exception(error)
    
    except Exception as e:
        raise e
    
    finally:
        client.close()



