using System.Drawing;

namespace Libs.Images
{
    internal interface IWuQuantizer
    {
        Image QuantizeImage(Bitmap image, int alphaThreshold, int alphaFader);
        Image QuantizeImage(Bitmap image, int alphaThreshold, int alphaFader, int maxColors);
    }
}