using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Libs.Helpers
{
    public static class ObjectHelpers
    {
        public static DataTable ToDataTable<T>(this IEnumerable<T> Linqlist)
        {
            try
            {
                DataTable dt = new DataTable();
                PropertyInfo[] columns = null;
                if (Linqlist == null) return dt;
                foreach (T Record in Linqlist)
                {
                    if (columns == null)
                    {
                        columns = ((System.Type)Record.GetType()).GetProperties();
                        foreach (PropertyInfo GetProperty in columns)
                        {
                            System.Type colType = GetProperty.PropertyType;
                            if ((colType.IsGenericType) && (colType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                                colType = colType.GetGenericArguments()[0];

                            dt.Columns.Add(new DataColumn(GetProperty.Name, colType));
                        }
                    }

                    DataRow dr = dt.NewRow();
                    foreach (PropertyInfo pinfo in columns)
                        dr[pinfo.Name] = pinfo.GetValue(Record, null) == null ? DBNull.Value : pinfo.GetValue(Record, null);

                    dt.Rows.Add(dr);
                }
                return dt;
            }
            catch (Exception ex)
            {
                SingletonProvider<LogWriter>.Instance.WriterLogInfo($"ToDataTable exception", ex);
                return default(DataTable);
            }

        }

        public static void InitFunction(Action func, string functionname)
        {
            SingletonProvider<LogWriter>.Instance.WriterLogInfo($"Start {func.Target.ToString()} {functionname}");
            Task.Run(func);
        }

        public static int ParseObjectToInt(object obj, int defVal = 0)
        {
            try
            {
                if (obj == null)
                    return defVal;
                return int.Parse(obj.ToString());
            }
            catch
            {
                return defVal;
            }
        }

        public static DateTime ParseObjectToDate(object obj, DateTime defVal = default(DateTime))
        {
            try
            {
                if (obj == null)
                    return defVal;
                return DateTime.Parse(obj.ToString());
            }
            catch
            {
                return defVal;
            }
        }

        public static string ParseObjectToString(object obj)
        {
            try
            {
                if (obj == null)
                    return string.Empty;
                return obj.ToString();
            }
            catch
            {
                return string.Empty;
            }
        }

        public static bool ParseObjectBool(object obj)
        {
            try
            {
                if (obj == null)
                    return false;
                return Convert.ToBoolean(obj.ToString());
            }
            catch
            {
                return false;
            }
        }

        public static double ConvertStringToIntervalTimer(string val)
        {
            double time = 0.0;
            try
            {
                if (string.IsNullOrEmpty(val) || string.IsNullOrWhiteSpace(val))
                    return 0.0;

                var lastchar = val.Substring(val.Length - 1).ToLower();
                var value = Regex.Match(val, @"\d+").Value;
                if (string.IsNullOrEmpty(value) || string.IsNullOrWhiteSpace(value))
                    return 0.0;
                switch (lastchar)
                {
                    case "s":
                        time = double.Parse(value) * 1000;
                        break;
                    case "m":
                        time = double.Parse(value) * 1000 * 60;
                        break;
                    case "h":
                        time = double.Parse(value) * 1000 * 60 * 60;
                        break;
                    case "d":
                        time = double.Parse(value) * 1000 * 60 * 60 * 24;
                        break;
                    default:
                        time = double.Parse(val) * 1000;
                        break;
                }
            }
            catch (Exception ex)
            {
                SingletonProvider<LogWriter>.Instance.WriterLogInfo($"ConvertStringToIntervalTimer exception", ex);
            }
            return time;
        }

        private readonly static DateTime START_UNIX_TIME = new DateTime(1970, 1, 1);

        public static int GetUnixTime(DateTime dt)
        {
            try
            {
                return dt.Year < ObjectHelpers.START_UNIX_TIME.Year ? 0 : Convert.ToInt32((dt - ObjectHelpers.START_UNIX_TIME).TotalSeconds);
            }
            catch
            {
                return 0;
            }
        }

        public static DateTime GetTimeUnix(int second = 0) => ObjectHelpers.START_UNIX_TIME.AddSeconds((double)second);



        public static string FirstLetterToUpper(this string str)
        {
            if (string.IsNullOrEmpty(str))
                return str;
            return str.Length > 1 ? char.ToUpper(str[0]).ToString() + str.Substring(1) : str.ToUpper();
        }

        public static string Encrypt(this string toEncrypt, bool useHashing = true, string haskey = null)
        {
            try
            {
                byte[] bytes = Encoding.UTF8.GetBytes(toEncrypt);
                string hasKey = haskey ?? "75021697-d03c-4c9f-bd9c-8cdb0630f37e";
                byte[] numArray;
                if (useHashing)
                {
                    MD5CryptoServiceProvider cryptoServiceProvider = new MD5CryptoServiceProvider();
                    numArray = cryptoServiceProvider.ComputeHash(Encoding.UTF8.GetBytes(hasKey));
                    cryptoServiceProvider.Clear();
                }
                else
                    numArray = Encoding.UTF8.GetBytes(hasKey);
                TripleDESCryptoServiceProvider cryptoServiceProvider1 = new TripleDESCryptoServiceProvider();
                cryptoServiceProvider1.Key = numArray;
                cryptoServiceProvider1.Mode = CipherMode.ECB;
                cryptoServiceProvider1.Padding = PaddingMode.PKCS7;
                byte[] inArray = cryptoServiceProvider1.CreateEncryptor().TransformFinalBlock(bytes, 0, bytes.Length);
                cryptoServiceProvider1.Clear();
                return Convert.ToBase64String(inArray, 0, inArray.Length);
            }
            catch (Exception ex)
            {
                SingletonProvider<LogWriter>.Instance.WriterLogInfo($"Encrypt exception", ex);
                return string.Empty;
            }
        }

        public static string Decrypt(this string cipherString, bool useHashing = true, string haskey = null)
        {
            try
            {
                byte[] inputBuffer = Convert.FromBase64String(cipherString);
                string hasKey = haskey ?? "75021697-d03c-4c9f-bd9c-8cdb0630f37e";
                byte[] numArray;
                if (useHashing)
                {
                    MD5CryptoServiceProvider cryptoServiceProvider = new MD5CryptoServiceProvider();
                    numArray = cryptoServiceProvider.ComputeHash(Encoding.UTF8.GetBytes(hasKey));
                    cryptoServiceProvider.Clear();
                }
                else
                    numArray = Encoding.UTF8.GetBytes(hasKey);
                TripleDESCryptoServiceProvider cryptoServiceProvider1 = new TripleDESCryptoServiceProvider();
                cryptoServiceProvider1.Key = numArray;
                cryptoServiceProvider1.Mode = CipherMode.ECB;
                cryptoServiceProvider1.Padding = PaddingMode.PKCS7;
                byte[] bytes = cryptoServiceProvider1.CreateDecryptor().TransformFinalBlock(inputBuffer, 0, inputBuffer.Length);
                cryptoServiceProvider1.Clear();
                return Encoding.UTF8.GetString(bytes);
            }
            catch (Exception ex)
            {
                SingletonProvider<LogWriter>.Instance.WriterLogInfo($"Encrypt exception", ex);
                return string.Empty;
            }

        }


        public static void FushRam()
        {
            try
            {
                GC.Collect();
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                        SetProcessWorkingSetSize32Bit(GetCurrentProcess(), -1, -1);
                    else
                        SetProcessWorkingSetSize64Bit(GetCurrentProcess(), -1, -1);
                }
            }
            catch (Exception ex)
            {

            }
        }

        [DllImport("KERNEL32.DLL", EntryPoint = "SetProcessWorkingSetSize", SetLastError = true, CallingConvention = CallingConvention.StdCall)]
        internal static extern bool SetProcessWorkingSetSize32Bit(IntPtr pProcess, int dwMinimumWorkingSetSize, int dwMaximumWorkingSetSize);

        [DllImport("KERNEL32.DLL", EntryPoint = "GetCurrentProcess", SetLastError = true, CallingConvention = CallingConvention.StdCall)]
        internal static extern IntPtr GetCurrentProcess();

        [DllImport("KERNEL32.DLL", EntryPoint = "SetProcessWorkingSetSize", SetLastError = true, CallingConvention = CallingConvention.StdCall)]
        internal static extern bool SetProcessWorkingSetSize64Bit(IntPtr pProcess, long dwMinimumWorkingSetSize, long dwMaximumWorkingSetSize);
    }
}
