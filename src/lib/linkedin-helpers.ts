/**
 * Pure helper functions for LinkedIn message step handling.
 * No external dependencies — safe to import in tests without mocking.
 */

/**
 * Returns true when a result from sendLinkedInMessage contains only
 * invitation_id with no message_id or chat_id, meaning the sender fell back
 * to the LinkedIn invitation endpoint instead of sending a real message.
 */
export function isPhantomMessageResult(result: any): boolean {
  return (
    result?.success === true &&
    !!result?.invitation_id &&
    !result?.message_id &&
    !result?.chat_id
  );
}

/**
 * Returns true when the current linkedin_message step follows at least one
 * prior completed linkedin_invitation or linkedin_message in this execution.
 * Used to decide whether the invite-endpoint fallback should be suppressed.
 */
export function isLinkedInMessageFollowUp(
  steps: any[],
  executionLog: any[],
  currentStepId: string,
): boolean {
  return steps.some((s: any) =>
    (s.step_type === 'linkedin_invitation' || s.step_type === 'linkedin_message') &&
    s.id !== currentStepId &&
    executionLog.some((l: any) => l.step_id === s.id && l.action === 'completed'),
  );
}
