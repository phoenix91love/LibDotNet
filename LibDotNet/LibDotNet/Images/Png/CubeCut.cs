namespace Libs.Images
{
   internal struct CubeCut
    {
        public readonly byte? Position;
        public readonly float Value;

        public CubeCut(byte? cutPoint, float result)
        {
            this.Position = cutPoint;
            this.Value = result;
        }
    }
}