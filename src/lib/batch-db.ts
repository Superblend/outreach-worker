import { SupabaseClient } from '@supabase/supabase-js';

export class BatchWriter {
  private buffer: any[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private tableName: string;
  private supabase: SupabaseClient;
  private onConflict?: string;

  constructor(supabase: SupabaseClient, tableName: string, options?: { onConflict?: string }) {
    this.supabase = supabase;
    this.tableName = tableName;
    this.onConflict = options?.onConflict;
  }

  async add(record: any) {
    this.buffer.push(record);
    if (this.buffer.length >= 50) {
      await this.flush();
    } else if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => this.flush(), 5000);
    }
  }

  async flush() {
    if (this.buffer.length === 0) return;
    const batch = this.buffer.splice(0, this.buffer.length);
    if (this.flushTimer) { clearTimeout(this.flushTimer); this.flushTimer = null; }

    const maxAttempts = 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      let error: any = null;
      if (this.tableName === 'unipile_step_results') {
        // Route through SECURITY DEFINER RPC: INSERT ... ON CONFLICT (execution_id, step_id)
        // DO UPDATE ... WHERE status <> 'success'. An existing 'success' row is immutable,
        // and real provider IDs (unipile_message_id, unipile_chat_id, tracking_id) are
        // merged via COALESCE so they are never lost. Replaces the silently-broken
        // upsert(..., { ignoreDuplicates: true }) which dropped retried-success rows.
        const results = await Promise.all(batch.map((r: any) =>
          this.supabase.rpc('upsert_step_result_safe', {
            p_execution_id: r.execution_id,
            p_step_id: r.step_id,
            p_lead_id: r.lead_id ?? null,
            p_status: r.status,
            p_executed_at: r.executed_at ?? new Date().toISOString(),
            p_response_data: r.response_data ?? null,
            p_unipile_message_id: r.unipile_message_id ?? null,
            p_unipile_chat_id: r.unipile_chat_id ?? null,
            p_tracking_id: r.tracking_id ?? null,
            p_step_type: r.step_type ?? null,
            p_error_message: r.error_message ?? null,
            p_contact_id: r.contact_id ?? null,
          })
        ));
        const failures = results.filter(r => r.error);
        if (failures.length > 0) {
          error = { message: `${failures.length}/${batch.length} upsert_step_result_safe calls failed`, sample: failures[0].error };
        }
      } else if (this.onConflict) {
        ({ error } = await this.supabase.from(this.tableName).upsert(batch, { onConflict: this.onConflict, ignoreDuplicates: true }));
      } else {
        ({ error } = await this.supabase.from(this.tableName).insert(batch));
      }
      if (!error) return;
      if (attempt < maxAttempts) {
        console.warn(`Batch write to ${this.tableName} failed (attempt ${attempt}/${maxAttempts}), retrying in 1s:`, error);
        await new Promise(resolve => setTimeout(resolve, 1000));
      } else {
        console.error(`Batch write to ${this.tableName} failed after ${maxAttempts} attempts:`, error, 'payload keys:', Object.keys(batch[0] || {}));
      }
    }
  }
}
