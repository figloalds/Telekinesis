#ifndef TK_WINFSP_DRIVER_MANIFEST_H
#define TK_WINFSP_DRIVER_MANIFEST_H

/* Exact official WinFsp 2026 Beta5 x64 artifacts. A missing manifest fails
   closed; version numbers alone do not establish the notification fixes. */
/* MSI SHA256:
   293ffb9bcc0b625c85079fe85fa77f8fe98170aba51a02ac9c83d58f6c3b0e2f.
   The source package/tag is v2.2B5; installation-time SxS IDs vary. */
#define TK_WINFSP_DLL_SHA256 L"94323b3de265c66a9b5428ee054313b9b14f179571201dfbaf5e7f15ba4918e3"
#define TK_WINFSP_SYS_SHA256 L"0a91fc3429fd5adf8b9c65976ac0c4ae924702be2cfbed856d90d2a7d99b0390"
#define TK_WINFSP_VERSION_MS ((2UL << 16) | 2UL)
#define TK_WINFSP_VERSION_LS ((26286UL << 16) | 0UL)

#endif
