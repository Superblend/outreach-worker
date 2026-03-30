export function normalizeAndReplace(text: string, lead: any, signature?: string): string {
  const vars: Record<string, string> = {
    firstName: lead.first_name || '', first_name: lead.first_name || '', firstname: lead.first_name || '',
    lastName: lead.last_name || '', last_name: lead.last_name || '', lastname: lead.last_name || '',
    company: lead.company || '', Company: lead.company || '',
    position: lead.position || lead.title || '', Position: lead.position || lead.title || '',
    title: lead.position || lead.title || '', Title: lead.position || lead.title || '',
    industry: lead.industry || '', Industry: lead.industry || '',
    ...(signature !== undefined ? { signature, Signature: signature } : {}),
  };
  return text.replace(/\{\{(\w+)\}\}/gi, (match, key) => vars[key] ?? vars[key.toLowerCase()] ?? match);
}
