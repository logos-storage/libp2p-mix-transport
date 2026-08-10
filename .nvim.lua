local nimble_bin = vim.fn.expand("~/.nimble/bin")

vim.lsp.config("nim_langserver", {
  cmd = { nimble_bin .. "/nimlangserver" },
  capabilities = {
    workspace = {
      configuration = false,
    },
  },
  settings = {
    nim = {
      nimsuggestPath = nimble_bin .. "/nimsuggest",
      projectMapping = {
        {
          projectFile = "mix_transport/libp2p_mix_transport.nim",
          fileRegex =
            "^mix_transport/.*[.]nim$",
        },
        {
          projectFile = "tests/test_all.nim",
          fileRegex = "^tests/.*[.]nim$",
        },
        {
          projectFile = "examples/mix_ping_tcp.nim",
          fileRegex = "^examples/mix_ping_tcp[.]nim$",
        },
        {
          projectFile = "examples/mix_ping_quic.nim",
          fileRegex = "^examples/mix_ping_quic[.]nim$",
        },
      },
    },
  },
})
