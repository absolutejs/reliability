import { defineManifest } from "@absolutejs/manifest";
import { Type } from "@sinclair/typebox";

export const manifest = defineManifest()({
  contract: 2,
  identity: {
    accent: "#5b8def",
    category: "infrastructure",
    description:
      "Atomic webhook inbox, checked-out transactions, and scoped idempotent operations for side-effecting integrations.",
    docsUrl: "https://github.com/absolutejs/reliability",
    name: "@absolutejs/reliability",
    tagline: "Make external side effects honest and recoverable.",
  },
  implements: [],
  settings: Type.Object({}),
  wiring: [],
});
