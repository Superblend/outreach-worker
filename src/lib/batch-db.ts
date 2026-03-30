import { SupabaseClient } from '@supabase/supabase-js';

export class BatchWriter {
  private buffer: any[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private tableName: string;
  private supabase: SupabaseClient;

  constructor(supabase: SupabaseClient, tableName: string) {
    this.supabase = supabase;
    this.tableName = tableName;
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
    const { error } = await this.supabase.from(this.tableName).insert(batch);
    if (error) console.error(`Batch insert to ${this.tableName} failed:`, error, 'payload keys:', Object.keys(batch[0] || {}));
  }
}
