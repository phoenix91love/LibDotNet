using Npgsql.Internal;
using System;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;

namespace Libs.Images
{
  
    public abstract class ImageType
    {
        public abstract ImageTypes Type { get; }
        public abstract void Save(Bitmap bitmap, string destPath, int? width = null, int? height = null, int quality = 75);

    }

    public class TypeWebP : ImageType
    {
        public override ImageTypes Type => ImageTypes.WebP;

        public override void Save(Bitmap bitmap, string destPath, int? width = null, int? height = null, int quality = 75)
        {
            using (var webp = new WebP())
            {
                var webp_byte = webp.EncodeLossless(bitmap);
                bitmap = webp.GetThumbnailQuality(webp_byte, width ?? bitmap.Width, height ?? bitmap.Height);
                webp.Save(bitmap, destPath, quality);
            }
        }
    }

    public class TypePng : ImageType
    {
        public override ImageTypes Type => ImageTypes.Png;

        public override void Save(Bitmap bitmap, string destPath, int? width = null, int? height = null, int quality = 75)
        {
            bitmap = bitmap.Resize(width ?? bitmap.Width, height ?? bitmap.Height);
            bitmap.Save(destPath, ImageFormat.Png, quality);
        }
    }

    public class TypeJpeg : ImageType
    {
        public override ImageTypes Type => ImageTypes.Jpeg;


        public override void Save(Bitmap bitmap, string destPath, int? width = null, int? height = null, int quality = 75)
        {
            bitmap = bitmap.Resize(width ?? bitmap.Width, height ?? bitmap.Height);
            bitmap.Save(destPath, ImageFormat.Jpeg, quality);
        }
    }
   



}