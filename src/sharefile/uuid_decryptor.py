import base64
from Crypto.Cipher import AES
import re

class UUIDDecryptor:
    # 与Java代码保持一致的参数
    ALGORITHM = "AES-CTR"
    KEY_SIZE = 16  # 128位密钥
    IV_LENGTH = 16  # 16字节IV

    def __init__(self, key):
        # 初始化16字节密钥，与Java处理方式一致
        key_bytes = key.encode('utf-8')
        # 如果密钥不足16字节则补全，如果过长则截断
        fixed_key = key_bytes.ljust(self.KEY_SIZE, b'\x00')[:self.KEY_SIZE]
        self.secret_key = fixed_key

    def decrypt_uuid(self, encrypted_str):
        try:
            # 补全Base32填充符，与Java的addPadding方法对应
            padded_str = self.add_padding(encrypted_str)
            
            # Base32解码
            combined = base64.b32decode(padded_str.encode('ascii'))
            
            # 分离IV和加密数据
            iv = combined[:self.IV_LENGTH]
            encrypted_data = combined[self.IV_LENGTH:]
            
            # 初始化AES CTR模式解密器
            cipher = AES.new(self.secret_key, AES.MODE_CTR, nonce=b'', initial_value=iv)
            
            # 解密
            decrypted_bytes = cipher.decrypt(encrypted_data)
            
            # 转换为16进制字符串并恢复UUID格式
            clean_uuid = self.bytes_to_hex(decrypted_bytes)
            return self.format_as_uuid(clean_uuid)
            
        except Exception as e:
            raise RuntimeError(f"解密失败: {str(e)}") from e

    @staticmethod
    def add_padding(s):
        """补全Base32需要的填充符"""
        mod = len(s) % 8
        if mod != 0:
            return s + '=' * (8 - mod)
        return s

    @staticmethod
    def bytes_to_hex(b):
        """字节数组转16进制字符串"""
        return ''.join(f'{byte:02x}' for byte in b).upper()

    @staticmethod
    def format_as_uuid(clean_uuid):
        """将32位字符串格式化为UUID格式"""
        if len(clean_uuid) != 32:
            raise ValueError("UUID解密后长度不正确")
            
        return f"{clean_uuid[0:8]}-{clean_uuid[8:12]}-{clean_uuid[12:16]}-{clean_uuid[16:20]}-{clean_uuid[20:32]}"

# # 测试代码
# if __name__ == "__main__":
#     # 使用与Java代码相同的密钥
#     secret_key = "16ByteSecureKey!"  # 16字节密钥
#     decryptor = UUIDDecryptor(secret_key)
    
#     # 替换为实际的加密字符串
#     # 注意：这里的示例值需要用Java代码实际生成的加密结果来测试
#     encrypted_uuid = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP"  # 示例值，实际使用时替换
    
#     try:
#         decrypted_uuid = decryptor.decrypt_uuid(encrypted_uuid)
#         print(f"解密结果: {decrypted_uuid}")
        
#         # 验证UUID格式
#         uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
#         if uuid_pattern.match(decrypted_uuid):
#             print("解密结果为有效的UUID格式")
#         else:
#             print("解密结果不是有效的UUID格式")
#     except Exception as e:
#         print(f"解密过程出错: {str(e)}")
