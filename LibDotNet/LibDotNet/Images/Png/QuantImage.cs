using System;
using System.Collections.Generic;
using System.Drawing;
using System.Text;

namespace Libs.Images
{
    public static class QuantImage
    {
        public static Image QuantizeImage(this Bitmap image) => new WuQuantizer().QuantizeImage(image);
        public static Image QuantizeImage(this Bitmap image, int alphaThreshold = 10, int alphaFader = 70) => new WuQuantizer().QuantizeImage(image, alphaThreshold, alphaFader);

        public static Image QuantizeImage(this Image image) => new WuQuantizer().QuantizeImage(new Bitmap(image));
        public static Image QuantizeImage(this Image image, int alphaThreshold=10, int alphaFader = 70) => new WuQuantizer().QuantizeImage(new Bitmap(image), alphaThreshold, alphaFader);

    }
}
