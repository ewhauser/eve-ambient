import type { FullAttentionBranch } from "@ewhauser/eve-ambient/protocol";
import { createHook } from "workflow";

export type AttentionStreamBranch = Pick<FullAttentionBranch, "branchKey" | "inputHash"> & {
  /** The complete by-value branch payload retained by the World event log. */
  readonly payload: unknown;
};

export type AttentionStreamCommand =
  | { readonly kind: "append"; readonly branch: AttentionStreamBranch }
  | { readonly kind: "close" };

export interface AttentionStreamResult {
  readonly kind: "closed";
  readonly address: string;
  readonly accepted: readonly AttentionStreamBranch[];
  readonly duplicates: number;
  readonly conflicts: readonly {
    readonly branchKey: string;
    readonly existingInputHash: string;
    readonly receivedInputHash: string;
  }[];
}

export interface AttentionStreamOwnerConflict {
  readonly kind: "owner-conflict";
  readonly address: string;
  readonly ownerRunId: string;
}

export type AttentionStreamWorkflowResult =
  | AttentionStreamResult
  | AttentionStreamOwnerConflict;

export function attentionStreamToken(address: string): string {
  return `eve-ambient:attention-stream:${address}`;
}

/**
 * Spike: model one Ambient correlation stream as one long-lived Workflow run.
 *
 * The reusable deterministic hook is the stream address. The workflow retains
 * full branch values and applies Ambient's key/hash rule during replay. A close
 * command exists only to make the retained state observable in this spike.
 */
export async function attentionStreamWorkflow(
  address: string,
): Promise<AttentionStreamWorkflowResult> {
  "use workflow";

  using input = createHook<AttentionStreamCommand>({
    token: attentionStreamToken(address),
    metadata: { kind: "eve-ambient-attention-stream", address },
  });
  const ownerConflict = await input.getConflict();
  if (ownerConflict !== null) {
    return {
      kind: "owner-conflict",
      address,
      ownerRunId: ownerConflict.runId,
    };
  }

  const accepted: AttentionStreamBranch[] = [];
  const conflicts: AttentionStreamResult["conflicts"][number][] = [];
  let duplicates = 0;

  for await (const command of input) {
    if (command.kind === "close") {
      return { kind: "closed", address, accepted, duplicates, conflicts };
    }

    const existing = accepted.find(
      (candidate) => candidate.branchKey === command.branch.branchKey,
    );
    if (existing === undefined) {
      accepted.push(command.branch);
      continue;
    }
    if (existing.inputHash === command.branch.inputHash) {
      duplicates += 1;
      continue;
    }
    conflicts.push({
      branchKey: command.branch.branchKey,
      existingInputHash: existing.inputHash,
      receivedInputHash: command.branch.inputHash,
    });
  }

  return { kind: "closed", address, accepted, duplicates, conflicts };
}
