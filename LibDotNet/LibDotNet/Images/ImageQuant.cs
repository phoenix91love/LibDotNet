using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.Runtime.CompilerServices;
using System.Text;
using ZstdSharp;

namespace Libs.Images
{
    public class QualityImage<IType> where IType : ImageType, new()
    {

        /// <summary>resize the webp image</summary>
        /// <param name="sourcePath">valid source image path</param>
        /// <param name="destPath">destination image path that saved image there</param>
        /// <param name="width">width that image resized to them</param>
        /// <param name="height">height that image resized to them</param>
        /// <param name="compress">compress image if that true</param>
        /// <returns>return true if do correctly else return false</returns>
        public static bool ResizeTo(string sourcePath, string destPath, int width, int height, bool compress = false)
        {
            try
            {
                if (String.IsNullOrEmpty(sourcePath) || String.IsNullOrEmpty(destPath) || width <= 0 || height <= 0) return false;
                using (WebP webp = new WebP())
                {
                    var type = new IType().Type;
                    if (type == ImageTypes.WebP)
                    {
                        var webp_byte = webp.LoadByte(sourcePath);
                        Bitmap bmp = compress ? webp.GetThumbnailFast(webp_byte, width, height) : webp.GetThumbnailQuality(webp_byte, width, height);
                        new IType().Save(bmp, destPath.ToValidImagePath(type), width, height);

                    }
                    else if (type == ImageTypes.Png)
                    {
                        Bitmap bmp = new Bitmap(sourcePath);
                        var webp_byte = webp.EncodeLossless(bmp);
                        bmp = compress ? webp.GetThumbnailFast(webp_byte, width, height) : webp.GetThumbnailQuality(webp_byte, width, height);
                        bmp.Save(destPath.ToValidImagePath(type), ImageFormat.Png);
                    }
                    else if (type == ImageTypes.Jpeg)
                    {
                        Bitmap bmp = new Bitmap(sourcePath);
                        var webp_byte = webp.EncodeLossless(bmp);
                        bmp = compress ? webp.GetThumbnailFast(webp_byte, width, height) : webp.GetThumbnailQuality(webp_byte, width, height);
                        bmp.Save(destPath.ToValidImagePath(type), ImageFormat.Jpeg);
                    }
                }
                return true;
            }
            catch { throw; }
        }

        /// <summary>convert image to new type image</summary>
        /// <param name="sourcePath">valid source image path</param>
        /// <param name="destPath">destination image path that saved image there</param> 
        /// <param name="width">width that image resized to them</param>
        /// <param name="height">height that image resized to them</param>
        /// <param name="quality">quality of converted image, between 0 and 100 <para>min quality : 0 </para><para>max quality : 100</para></param>
        /// <returns>return true if do correctly else return false</returns>
        public static bool ChangeTo(string sourcePath, string destPath, int? width = null, int? height = null, int quality = 100)
        {
            try
            {
                if (String.IsNullOrEmpty(sourcePath) || String.IsNullOrEmpty(destPath) || height <= 0 || width <= 0) return false;
                quality = quality <= 0 || quality > 100 ? 100 : quality;
                using (WebP webp = new WebP())
                {
                    var image_byte = webp.LoadByte(sourcePath);
                    var typeSourceFormat = ImageHelper.GetImageFormat(image_byte);
                    var type = new IType().Type;

                    if (typeSourceFormat == ImageTypes.WebP)
                        new IType().Save(webp.Decode(image_byte), destPath.ToValidImagePath(type), width, height, quality);
                    else
                        new IType().Save(new Bitmap(sourcePath), destPath.ToValidImagePath(type), width, height, quality);
                }


                return true;
            }
            catch { throw; }
        }




    }
}
