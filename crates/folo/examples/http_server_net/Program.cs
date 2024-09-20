using System.Buffers;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://0.0.0.0:1234");

var app = builder.Build();

app.MapGet("/", () => Results.Text("Hello, World!"));

// 20 KB
var response_20_kb = new byte[20 * 1024];
app.MapGet("/20kb", () => Results.Bytes(response_20_kb));

// 64 MB
var response_64_mb = new byte[64 * 1024 * 1024];
app.MapGet("/64mb", () => Results.Bytes(response_64_mb));


var buffer_1_mb = new byte[1024 * 1024];

app.MapGet("/infinite", async (HttpContext context) =>
{
    while (true)
    {
        await context.Response.Body.WriteAsync(buffer_1_mb, context.RequestAborted);
    }
});

app.Run();
