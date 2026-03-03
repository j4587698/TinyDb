using System.Buffers;

namespace TinyDb.Utils;

internal interface IPatchableBufferWriter : IBufferWriter<byte>
{
    int WrittenCount { get; }
    void WriteInt32LittleEndianAt(int offset, int value);
}
