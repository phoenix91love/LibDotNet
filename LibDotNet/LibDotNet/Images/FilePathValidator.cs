using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace Libs.Images
{
    internal static class FilePathValidator
    {
        private static readonly HashSet<string> ValidImageExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "jpg", "jpeg", "png", "gif", "bmp", "tiff", "webp", "svg", "ico"
    };

        private static string RemoveInvalidFileNameChars(string filename)
        {
            if (string.IsNullOrEmpty(filename))
                return filename;

            // Sử dụng regex để loại bỏ các ký tự không hợp lệ
            string invalidChars = Regex.Escape(new string(Path.GetInvalidFileNameChars()));
            string invalidRegStr = string.Format(@"[{0}]", invalidChars);

            return Regex.Replace(filename, invalidRegStr, "_");
        }

        // Phiên bản nâng cao với nhiều tùy chọn
        internal static string ValidateFilePathAdvanced(string filePath, ImageTypes fileType, bool preserveOriginalExtension = false)
        {
            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("Đường dẫn file không được để trống");

            if (!ValidImageExtensions.Contains(fileType.ToString().ToLower()))
                throw new ArgumentException($"Loại file không hợp lệ: {fileType}");

            string directory = Path.GetDirectoryName(filePath);
            string filename = Path.GetFileName(filePath);

            if (string.IsNullOrWhiteSpace(filename))
                throw new ArgumentException("Tên file không được để trống");

            string name = Path.GetFileNameWithoutExtension(filename);
            string currentExt = Path.GetExtension(filename)?.TrimStart('.').ToLower();

            name = RemoveInvalidFileNameChars(name);

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Tên file không hợp lệ sau khi xử lý");

            string finalExt;

            if (preserveOriginalExtension && !string.IsNullOrEmpty(currentExt) && ValidImageExtensions.Contains(currentExt))
            {
                // Giữ nguyên phần mở rộng gốc nếu hợp lệ
                finalExt = currentExt;
            }
            else if (string.IsNullOrEmpty(currentExt) || !ValidImageExtensions.Contains(currentExt))
            {
                // Sử dụng loại file được chỉ định
                finalExt = fileType.ToString().ToLower();
            }
            else
            {
                finalExt = fileType.ToString().ToLower();
            }

            string finalFilename = $"{name}.{finalExt}";

            return !string.IsNullOrEmpty(directory)
                ? Path.Combine(directory, finalFilename)
                : finalFilename;
        }

        internal static string ToValidImagePath(this string filePath, ImageTypes fileType, bool preserveOriginalExtension = false) => FilePathValidator.ValidateFilePathAdvanced(filePath, fileType, preserveOriginalExtension);
    }
   
    
}
