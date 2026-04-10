export function normalizeAndReplace(text: string, lead: any, signature?: string): string {
  const fullName = `${lead.first_name || ''} ${lead.last_name || ''}`.trim();
  const vars: Record<string, string> = {
    firstName: lead.first_name || '', first_name: lead.first_name || '', firstname: lead.first_name || '',
    lastName: lead.last_name || '', last_name: lead.last_name || '', lastname: lead.last_name || '',
    fullName, full_name: fullName, fullname: fullName,
    company: lead.company || '', Company: lead.company || '',
    position: lead.position || lead.title || '', Position: lead.position || lead.title || '',
    title: lead.position || lead.title || '', Title: lead.position || lead.title || '',
    industry: lead.industry || '', Industry: lead.industry || '',
    linkedin: lead.linkedin || '', LinkedIn: lead.linkedin || '', linkedin_url: lead.linkedin || '',
    website: lead.website || '', Website: lead.website || '',
    phone: lead.phone || '', Phone: lead.phone || '',
    email: lead.email || '', Email: lead.email || '',
    companySize: lead.company_size || '', company_size: lead.company_size || '',
    ...(signature !== undefined ? { signature, Signature: signature } : {}),
  };

  // Spread custom_fields into vars (won't override built-in keys)
  if (lead.custom_fields && typeof lead.custom_fields === 'object') {
    for (const [key, val] of Object.entries(lead.custom_fields)) {
      if (!vars[key]) {
        const stringVal = (val && typeof val === 'object')
          ? (Array.isArray(val) ? val.join(', ') : ('min' in (val as any) && 'max' in (val as any) ? `${(val as any).min}-${(val as any).max}` : JSON.stringify(val)))
          : String(val || '');
        vars[key] = stringVal;
      }
    }
  }

  // Replace all {{variable}} patterns with dynamic fallback to lead object
  return text.replace(/\{\{(\w+)\}\}/gi, (match, key) => {
    return vars[key] ?? vars[key.toLowerCase()] ?? lead[key] ?? lead[key.toLowerCase()] ?? match;
  });
}
