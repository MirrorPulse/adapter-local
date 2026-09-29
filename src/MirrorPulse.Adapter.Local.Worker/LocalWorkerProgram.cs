using System.Security.Cryptography;
using System.Text.Json;
using System.Globalization;
using System.Text;
using MirrorPulse.Adapter.Sdk;

namespace MirrorPulse.Adapter.Local.Worker;

public static class LocalWorkerProgram
{
    private static readonly JsonSerializerOptions ConfigurationOptions = new(JsonSerializerDefaults.General);

    public static async Task<int> RunAsync(IReadOnlyList<string> args,
        CancellationToken cancellationToken = default)
    {
        AdapterWorkerProcessArguments arguments;
        try
        {
            arguments = AdapterWorkerProcessArguments.Parse(args);
        }
        catch (ArgumentException)
        {
            return 2;
        }

        await using AdapterNamedPipeClient pipe = await AdapterNamedPipeClient.ConnectAsync(
            arguments.PipeName, TimeSpan.FromSeconds(15), cancellationToken).ConfigureAwait(false);
        var channel = new AdapterControlChannel(pipe, arguments.InstanceId, arguments.WorkerSessionId);
        Guid helloId = Guid.NewGuid();
        await channel.SendAsync("Hello", helloId, false, new
        {
            adapterId = "com.mirrorpulse.adapter.local",
            minimumProtocolVersion = 1,
            maximumProtocolVersion = 1,
        }, cancellationToken).ConfigureAwait(false);

        try
        {
            AdapterControlFrame ready = await channel.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (ready.MessageType != "Ready" || !ready.IsResponse || ready.RequestId != helloId)
            {
                throw new InvalidDataException("The Host did not accept the Local Worker handshake.");
            }

            Dictionary<string, string> configuration = ready.Payload.Deserialize<Dictionary<string, string>>(
                    ConfigurationOptions)
                ?? throw new InvalidDataException("The Host did not provide Local Worker configuration.");
            string sourceDirectory = configuration.GetValueOrDefault("sourceDirectory")
                ?? throw new InvalidDataException("The Local Worker sourceDirectory is missing.");
            var paths = new LocalWorkerPaths(sourceDirectory);
            await channel.SendAsync("Connected", helloId, false, new { sourceDirectory = paths.Root },
                cancellationToken).ConfigureAwait(false);
            var protocol = new LocalWorkerTransferProtocol(channel, paths,
                arguments.InstanceId, arguments.WorkerSessionId);
            while (true)
            {
                AdapterControlFrame command = await channel.ReadAsync(cancellationToken).ConfigureAwait(false);
                if (command.IsResponse)
                {
                    throw new InvalidDataException("The Host sent an unexpected response to the Local Worker.");
                }

                if (command.MessageType == "Stop")
                {
                    await channel.SendAsync("Stopped", command.RequestId, true, new { }, cancellationToken)
                        .ConfigureAwait(false);
                    return 0;
                }

                await protocol.HandleAsync(command, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception exception)
        {
            string code = exception switch
            {
                InvalidDataException or JsonException => "InvalidConfiguration",
                UnauthorizedAccessException => "AccessDenied",
                FileNotFoundException or DirectoryNotFoundException => "SourceUnavailable",
                IOException => "LocalIoFailure",
                _ => "WorkerFailure",
            };
            await channel.SendAsync("Error", helloId, false, new { code }, CancellationToken.None)
                .ConfigureAwait(false);
            return 1;
        }
    }
}

internal sealed class LocalWorkerTransferProtocol(
    AdapterControlChannel channel,
    LocalWorkerPaths paths,
    Guid instanceId,
    Guid workerSessionId)
{
    private const int MaximumRangeBytes = 1024 * 1024;

    public async Task HandleAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        try
        {
            switch (command.MessageType)
            {
                case "Stat":
                    await StatAsync(command, cancellationToken).ConfigureAwait(false);
                    break;
                case "ReadRange":
                    await ReadRangeAsync(command, cancellationToken).ConfigureAwait(false);
                    break;
                case "Upload":
                    await UploadAsync(command, cancellationToken).ConfigureAwait(false);
                    break;
                case "List":
                    await ListAsync(command, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidDataException("The Local Worker received an unsupported command.");
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            string code = exception switch
            {
                LocalRevisionConflictException => "RemoteConflict",
                InvalidDataException or ArgumentException or JsonException => "InvalidRequest",
                UnauthorizedAccessException => "AccessDenied",
                FileNotFoundException => "SourceUnavailable",
                _ => "RetryableTransferFailure",
            };
            await channel.SendAsync("OperationError", command.RequestId, true, new { code },
                CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task StatAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString()
            ?? throw new InvalidDataException("The Local stat path is missing.");
        string resolved = paths.Resolve(path);
        if (!File.Exists(resolved))
        {
            await channel.SendAsync("StatResult", command.RequestId, true, new { revision = (string?)null },
                cancellationToken).ConfigureAwait(false);
            return;
        }

        await channel.SendAsync("StatResult", command.RequestId, true,
            new { revision = Revision(resolved), length = new FileInfo(resolved).Length }, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task ListAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString() ?? string.Empty;
        int pageSize = command.Payload.GetProperty("pageSize").GetInt32();
        if (pageSize is < 1 or > 512)
        {
            throw new InvalidDataException("The Local directory page size is invalid.");
        }

        int offset = 0;
        if (command.Payload.TryGetProperty("cursor", out JsonElement cursor) &&
            cursor.ValueKind is not JsonValueKind.Null && !string.IsNullOrEmpty(cursor.GetString()))
        {
            string text = Encoding.UTF8.GetString(Convert.FromBase64String(cursor.GetString()!));
            if (!int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out offset) || offset < 0)
            {
                throw new InvalidDataException("The Local directory cursor is invalid.");
            }
        }

        string directory = paths.ResolveDirectory(path);
        if (!Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException(directory);
        }

        string[] children = Directory.EnumerateFileSystemEntries(directory)
            .OrderBy(item => Path.GetFileName(item), StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => Path.GetFileName(item), StringComparer.Ordinal)
            .ToArray();
        if (offset > children.Length)
        {
            throw new InvalidDataException("The Local directory cursor is past the end of the page.");
        }

        string root = paths.Root;
        var entries = new List<object>(Math.Min(pageSize, children.Length - offset));
        foreach (string child in children.Skip(offset).Take(pageSize))
        {
            bool isDirectory = Directory.Exists(child);
            FileInfo? file = isDirectory ? null : new FileInfo(child);
            string relative = Path.GetRelativePath(root, child).Replace(Path.DirectorySeparatorChar, '/');
            DateTime creation = File.GetCreationTimeUtc(child);
            DateTime lastWrite = File.GetLastWriteTimeUtc(child);
            entries.Add(new
            {
                remoteId = relative,
                remoteRevision = isDirectory
                    ? lastWrite.Ticks.ToString(CultureInfo.InvariantCulture)
                    : Revision(child),
                itemKind = isDirectory ? "Directory" : "File",
                relativePath = relative,
                length = file?.Length,
                creationTime = new DateTimeOffset(creation, TimeSpan.Zero),
                lastWriteTime = new DateTimeOffset(lastWrite, TimeSpan.Zero),
                isDeleted = false,
            });
        }

        int nextOffset = offset + entries.Count;
        bool complete = nextOffset >= children.Length;
        await channel.SendAsync("DirectoryPage", command.RequestId, true, new
        {
            entries,
            cursor = complete ? null : Convert.ToBase64String(Encoding.UTF8.GetBytes(nextOffset.ToString(
                CultureInfo.InvariantCulture))),
            isComplete = complete,
        }, cancellationToken).ConfigureAwait(false);
    }

    private async Task ReadRangeAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString()
            ?? throw new InvalidDataException("The Local read path is missing.");
        long offset = command.Payload.GetProperty("offset").GetInt64();
        int length = command.Payload.GetProperty("length").GetInt32();
        if (offset < 0 || length is < 0 or > MaximumRangeBytes)
        {
            throw new InvalidDataException("The Local read range is invalid.");
        }

        string resolved = paths.Resolve(path);
        await using var input = new FileStream(resolved, FileMode.Open, FileAccess.Read, FileShare.ReadWrite,
            64 * 1024, FileOptions.Asynchronous);
        if (offset > input.Length || length > input.Length - offset)
        {
            throw new InvalidDataException("The Local read range exceeds the file.");
        }

        input.Position = offset;
        byte[] bytes = new byte[length];
        await input.ReadExactlyAsync(bytes, cancellationToken).ConfigureAwait(false);
        Guid streamId = Guid.NewGuid();
        await channel.SendAsync("ReadRangeReady", command.RequestId, true,
            new { streamId, length }, cancellationToken).ConfigureAwait(false);
        await channel.SendChunkAsync(new AdapterBinaryChunk(command.RequestId, instanceId, workerSessionId,
            streamId, offset, bytes, true), cancellationToken).ConfigureAwait(false);
    }

    private async Task UploadAsync(AdapterControlFrame command, CancellationToken cancellationToken)
    {
        string path = command.Payload.GetProperty("path").GetString()
            ?? throw new InvalidDataException("The Local upload path is missing.");
        string? expectedRevision = command.Payload.GetProperty("expectedRevision").GetString();
        long length = command.Payload.GetProperty("length").GetInt64();
        Guid streamId = command.Payload.GetProperty("streamId").GetGuid();
        if (length < 0 || streamId == Guid.Empty)
        {
            throw new InvalidDataException("The Local upload length or stream ID is invalid.");
        }

        string destination = paths.Resolve(path);
        string transferRoot = Environment.GetEnvironmentVariable("MP_TRANSFER_CACHE_DIR")
            ?? throw new InvalidDataException("The Host did not provide a transfer cache directory.");
        Directory.CreateDirectory(transferRoot);
        string staged = Path.Combine(transferRoot, $"local-{command.RequestId:N}.tmp");
        await channel.SendAsync("UploadReady", command.RequestId, true, new { streamId }, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await using (var output = new FileStream(staged, FileMode.Create, FileAccess.Write, FileShare.None,
                64 * 1024, FileOptions.Asynchronous))
            {
                long received = 0;
                while (true)
                {
                    AdapterBinaryChunk chunk = await channel.ReadChunkAsync(cancellationToken).ConfigureAwait(false);
                    if (chunk.RequestId != command.RequestId || chunk.StreamId != streamId ||
                        chunk.Offset != received || chunk.Data.Length > length - received)
                    {
                        throw new InvalidDataException("The Local upload chunk is out of order or too large.");
                    }

                    await output.WriteAsync(chunk.Data, cancellationToken).ConfigureAwait(false);
                    received += chunk.Data.Length;
                    await channel.SendAsync("TransferProgress", command.RequestId, false,
                        new { operation = "upload", bytesTransferred = received, totalBytes = length,
                            phase = "Transferring" }, cancellationToken).ConfigureAwait(false);
                    if (chunk.EndOfStream)
                    {
                        if (received != length)
                        {
                            throw new InvalidDataException("The Local upload ended before its declared size.");
                        }

                        break;
                    }
                }
            }

            string? current = File.Exists(destination) ? Revision(destination) : null;
            if (!string.Equals(current, expectedRevision, StringComparison.Ordinal))
            {
                throw new LocalRevisionConflictException();
            }

            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Move(staged, destination, overwrite: true);
            await channel.SendAsync("UploadComplete", command.RequestId, true,
                new { revision = Revision(destination) }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            File.Delete(staged);
        }
    }

    private static string Revision(string path)
    {
        FileInfo file = new(path);
        string digest = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))[..8]);
        return $"{file.Length}:{file.LastWriteTimeUtc.Ticks}:{digest}";
    }
}

internal sealed class LocalRevisionConflictException : IOException
{
    public LocalRevisionConflictException() : base("The local source changed before upload completion.") { }
}
