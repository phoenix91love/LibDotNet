using System;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.Linq;

namespace Libs.Images
{
    internal static class ImageHelper
    {
        internal static Bitmap Resize(this Bitmap original, int width, int height, ResizeMode mode = ResizeMode.Stretch, bool highQuality = true)
        {
            return mode switch
            {
                ResizeMode.Stretch => ResizeStretch(original, width, height, highQuality),
                ResizeMode.Proportional => ResizeProportional(original, width, height, highQuality),
                ResizeMode.Crop => ResizeCrop(original, width, height, highQuality),
                _ => throw new ArgumentException("Invalid resize mode")
            };
        }

        internal static void Save(this Bitmap bitmap, string filePath, ImageFormat format, int quality = 90)
        {
            var encoder = GetEncoder(format);
            if (encoder != null && quality < 100)
            {
                var parameters = new EncoderParameters(1);
                parameters.Param[0] = new EncoderParameter(Encoder.Quality, quality);
                bitmap.Save(filePath, encoder, parameters);
            }
            else
            {
                bitmap.Save(filePath, format);
            }
        }

        private static Bitmap ResizeStretch(Bitmap original, int width, int height, bool highQuality)
        {
            var result = new Bitmap(width, height, original.PixelFormat);
            using var graphics = GetHighQualityGraphics(result, highQuality);
            graphics.DrawImage(original, 0, 0, width, height);
            return result;
        }

        private static Bitmap ResizeProportional(Bitmap original, int maxWidth, int maxHeight, bool highQuality)
        {
            var ratio = Math.Min((double)maxWidth / original.Width, (double)maxHeight / original.Height);
            var newWidth = (int)(original.Width * ratio);
            var newHeight = (int)(original.Height * ratio);

            var result = new Bitmap(newWidth, newHeight, original.PixelFormat);
            using var graphics = GetHighQualityGraphics(result, highQuality);
            graphics.DrawImage(original, 0, 0, newWidth, newHeight);
            return result;
        }

        private static Bitmap ResizeCrop(Bitmap original, int width, int height, bool highQuality)
        {
            var result = new Bitmap(width, height, original.PixelFormat);

            // Tính toán tỉ lệ và vị trí crop
            var sourceRatio = (double)original.Width / original.Height;
            var destRatio = (double)width / height;

            int sourceX = 0, sourceY = 0, sourceWidth = original.Width, sourceHeight = original.Height;

            if (sourceRatio > destRatio)
            {
                // Crop theo chiều rộng
                sourceWidth = (int)(original.Height * destRatio);
                sourceX = (original.Width - sourceWidth) / 2;
            }
            else
            {
                // Crop theo chiều cao
                sourceHeight = (int)(original.Width / destRatio);
                sourceY = (original.Height - sourceHeight) / 2;
            }

            using var graphics = GetHighQualityGraphics(result, highQuality);
            graphics.DrawImage(original,
                new Rectangle(0, 0, width, height),
                new Rectangle(sourceX, sourceY, sourceWidth, sourceHeight),
                GraphicsUnit.Pixel);

            return result;
        }

        private static Graphics GetHighQualityGraphics(Bitmap bitmap, bool highQuality)
        {
            var graphics = Graphics.FromImage(bitmap);

            if (highQuality)
            {
                graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
                graphics.SmoothingMode = SmoothingMode.HighQuality;
                graphics.PixelOffsetMode =PixelOffsetMode.HighQuality;
                graphics.CompositingQuality = CompositingQuality.HighQuality;
            }

            return graphics;
        }


        private static ImageCodecInfo GetEncoder(ImageFormat format) => ImageCodecInfo.GetImageDecoders().FirstOrDefault(codec => codec.FormatID == format.Guid);

        internal static ImageTypes GetImageFormat(byte[] data)
        {
            if (data == null || data.Length < 8)
                return ImageTypes.Unknown;

            // WebP
            if (data.Length >= 12 &&
                data[0] == 0x52 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x46 &&
                data[8] == 0x57 && data[9] == 0x45 && data[10] == 0x42 && data[11] == 0x50)
                return ImageTypes.WebP;

            // PNG
            if (data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47)
                return ImageTypes.Png;

            // JPEG
            if (data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF)
                return ImageTypes.Jpeg;

            // GIF
            if (data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46)
                return ImageTypes.Gif;

            // BMP
            if (data[0] == 0x42 && data[1] == 0x4D)
                return ImageTypes.Bmp;

            return ImageTypes.Unknown;
        }
    }

    internal enum ResizeMode
    {
        Stretch,      // Kéo dãn
        Proportional, // Giữ tỉ lệ
        Crop          // Cắt ảnh
    }
    /// <summary>
    /// Type Image need support
    /// </summary>
    public enum ImageTypes
    {
        Bmp,
        WebP,
        Png,
        Jpg,
        Jpeg,
        Gif,
        Tiff,
        Svg,
        Ico,
        Unknown
    }

}
