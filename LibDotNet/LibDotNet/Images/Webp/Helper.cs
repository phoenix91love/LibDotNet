using System.IO;
using System.Net;
using System.Text;

namespace Libs.Images
{
    internal class Helper
    {
        internal static Stream DownloadImage(string imageUrl)
        {
            using (WebClient client = new WebClient())
            {
                client.Encoding = Encoding.UTF8;
                client.Headers[HttpRequestHeader.AcceptEncoding] = "gzip, deflate";
                client.Headers[HttpRequestHeader.UserAgent] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36";

                // Bật chế độ sử dụng buffer để tối ưu hiệu năng
                client.UseDefaultCredentials = false;
                client.Proxy = null; // Tắt Proxy nếu không cần
               
                return client.OpenRead(imageUrl);
            }
        }

        internal static byte[] ReadByte(Stream input)
        {
            byte[] buffer = new byte[16 * 1024];
            using (MemoryStream ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                    ms.Write(buffer, 0, read);

                return ms.ToArray();
            }
        }
    }
}
