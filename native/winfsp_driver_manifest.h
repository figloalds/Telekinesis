#ifndef TK_WINFSP_DRIVER_MANIFEST_H
#define TK_WINFSP_DRIVER_MANIFEST_H

/* Minimum WinFsp file version: 2.2.26286 (2026 Beta5), the first release with
   the notification/rename deadlock fix (WinFsp PR #669). Newer releases are
   accepted; this is a floor, not an allowlist. */
#define TK_WINFSP_MIN_VERSION_MS ((2UL << 16) | 2UL)
#define TK_WINFSP_MIN_VERSION_LS ((26286UL << 16) | 0UL)

#endif
