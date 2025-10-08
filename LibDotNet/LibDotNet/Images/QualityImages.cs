using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.Runtime.CompilerServices;
using System.Text;
using ZstdSharp;

namespace Libs.Images
{
    /// <summary>
    /// Optimize current image
    /// </summary>
    public static class QualityImages
    {
        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <returns></returns>
        public static Image QuantizeImage(this Bitmap image) => new WuQuantizer().QuantizeImage(image);
        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <param name="alphaThreshold">alpha Threshold</param>
        /// <param name="alphaFader"></param>
        /// <returns></returns>
        public static Image QuantizeImage(this Bitmap image, int alphaThreshold = 10, int alphaFader = 70) => new WuQuantizer().QuantizeImage(image, alphaThreshold, alphaFader);

        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <returns></returns>
        public static Image QuantizeImage(this Image image) => new WuQuantizer().QuantizeImage(new Bitmap(image));

        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <param name="alphaThreshold">alpha Threshold</param>
        /// <param name="alphaFader"></param>
        public static Image QuantizeImage(this Image image, int alphaThreshold = 10, int alphaFader = 70) => new WuQuantizer().QuantizeImage(new Bitmap(image), alphaThreshold, alphaFader);

        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <returns></returns>
        public static Bitmap GetQuantizedBitmap(this Bitmap image) => new Bitmap(QuantizeImage(image));

        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <param name="alphaThreshold">alpha Threshold</param>
        /// <param name="alphaFader"></param>
        public static Bitmap GetQuantizedBitmap(this Bitmap image, int alphaThreshold = 10, int alphaFader = 70) => new Bitmap(QuantizeImage(image, alphaThreshold, alphaFader));

        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <returns></returns>
        public static Bitmap GetQuantizedBitmap(this Image image) => GetQuantizedBitmap(new Bitmap(image));
        /// <summary>
        /// Quantize Image
        /// </summary>
        /// <param name="image">Current bitmap need optimize</param>
        /// <param name="alphaThreshold">alpha Threshold</param>
        /// <param name="alphaFader"></param>
        public static Bitmap GetQuantizedBitmap(this Image image, int alphaThreshold = 10, int alphaFader = 70) => GetQuantizedBitmap(new Bitmap(image), alphaThreshold, alphaFader);
    }

    /// <summary>
    /// Optimize image, save change to other type image
    /// </summary>
    /// <typeparam name="IType">ImageType: is target change image</typeparam>
    public class QualityImages<IType> where IType : ImageType, new()
    {

        /// <summary>resize the webp image</summary>
        /// <param name="sourcePath">valid source image path</param>
        /// <param name="destPath">destination image path that saved image there</param>
        /// <param name="width">width that image resized to them</param>
        /// <param name="height">height that image resized to them</param>
        /// <param name="compress">compress image if that true</param>
        /// <returns>return true if do correctly else return false</returns>
        public static bool Optimize(string sourcePath, string destPath, int? width = null, int? height = null, bool compress = false)
        {
            try
            {
                if (String.IsNullOrEmpty(sourcePath) || String.IsNullOrEmpty(destPath) || width <= 0 || height <= 0) return false;
                using (WebP webp = new WebP())
                {
                    var image_byte = webp.LoadByte(sourcePath);
                    var typeSourceFormat = ImageHelper.GetImageFormat(image_byte);
                    var type = new IType().Type;

                    if (typeSourceFormat == ImageTypes.WebP)
                        new IType().Save(webp.Decode(image_byte), destPath.ToValidImagePath(type), width, height, compress);
                    else
                        new IType().Save(new Bitmap(sourcePath), destPath.ToValidImagePath(type), width, height, compress);
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

        /// <summary>resize the webp image</summary>
        /// <param name="source">valid source image bitmap</param>
        /// <param name="destPath">destination image path that saved image there</param>
        /// <param name="width">width that image resized to them</param>
        /// <param name="height">height that image resized to them</param>
        /// <param name="compress">compress image if that true</param>
        /// <returns>return true if do correctly else return false</returns>
        public static bool SaveTo(Bitmap source, string destPath, int? width = null, int? height = null, bool compress = false)
        {
            try
            {
                if (source == null || String.IsNullOrEmpty(destPath) || width <= 0 || height <= 0) return false;
                using (WebP webp = new WebP())
                {
                    var type = new IType().Type;

                    if (type == ImageTypes.WebP)
                        new IType().Save(source, destPath.ToValidImagePath(type), width, height, compress);
                    else
                        new IType().Save(source, destPath.ToValidImagePath(type), width, height, compress);
                }
                return true;
            }
            catch { throw; }
        }

        /// <summary>convert image to new type image</summary>
        /// <param name="source">valid source image bitmap</param>
        /// <param name="destPath">destination image path that saved image there</param> 
        /// <param name="width">width that image resized to them</param>
        /// <param name="height">height that image resized to them</param>
        /// <param name="quality">quality of converted image, between 0 and 100 <para>min quality : 0 </para><para>max quality : 100</para></param>
        /// <returns>return true if do correctly else return false</returns>
        public static bool SaveTo(Bitmap source, string destPath, int? width = null, int? height = null, int quality = 100)
        {
            try
            {
                if (source == null || String.IsNullOrEmpty(destPath) || width <= 0 || height <= 0) return false;
                using (WebP webp = new WebP())
                {
                    var type = new IType().Type;

                    if (type == ImageTypes.WebP)
                        new IType().Save(source, destPath.ToValidImagePath(type), width, height, quality);
                    else
                        new IType().Save(source, destPath.ToValidImagePath(type), width, height, quality);
                }
                return true;
            }
            catch { throw; }
        }

        /// <summary>resize the webp image</summary>
        /// <param name="source">valid source image</param>
        /// <param name="destPath">destination image path that saved image there</param>
        /// <param name="width">width that image resized to them</param>
        /// <param name="height">height that image resized to them</param>
        /// <param name="compress">compress image if that true</param>
        /// <returns>return true if do correctly else return false</returns>
        public static bool SaveTo(Image source, string destPath, int? width = null, int? height = null, bool compress = false)
        {
            try
            {
                if (source == null || String.IsNullOrEmpty(destPath) || width <= 0 || height <= 0) return false;
                using (WebP webp = new WebP())
                {
                    var type = new IType().Type;

                    if (type == ImageTypes.WebP)
                        new IType().Save(new Bitmap(source), destPath.ToValidImagePath(type), width, height, compress);
                    else
                        new IType().Save(new Bitmap(source), destPath.ToValidImagePath(type), width, height, compress);
                }
                return true;
            }
            catch { throw; }
        }

        /// <summary>convert image to new type image</summary>
        /// <param name="source">valid source image</param>
        /// <param name="destPath">destination image path that saved image there</param> 
        /// <param name="width">width that image resized to them</param>
        /// <param name="height">height that image resized to them</param>
        /// <param name="quality">quality of converted image, between 0 and 100 <para>min quality : 0 </para><para>max quality : 100</para></param>
        /// <returns>return true if do correctly else return false</returns>
        public static bool SaveTo(Image source, string destPath, int? width = null, int? height = null, int quality = 100)
        {
            try
            {
                if (source == null || String.IsNullOrEmpty(destPath) || width <= 0 || height <= 0) return false;
                using (WebP webp = new WebP())
                {
                    var type = new IType().Type;

                    if (type == ImageTypes.WebP)
                        new IType().Save(new Bitmap(source), destPath.ToValidImagePath(type), width, height, quality);
                    else
                        new IType().Save(new Bitmap(source), destPath.ToValidImagePath(type), width, height, quality);
                }
                return true;
            }
            catch { throw; }
        }
    }
}
