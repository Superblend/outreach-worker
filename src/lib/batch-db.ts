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
      const { error } = this.onConflict
        ? await this.supabase.from(this.tableName).upsert(batch, { onConflict: this.onConflict, ignoreDuplicates: true })
        : await this.supabase.from(this.tableName).insert(batch);
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
