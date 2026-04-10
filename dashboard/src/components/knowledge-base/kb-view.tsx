'use client';

import { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { renderMarkdown } from '@/lib/render-markdown';

interface KnowledgeBaseViewProps {
  content: string;
  org: string;
  filePath: string;
}

export function KnowledgeBaseView({ content, org, filePath }: KnowledgeBaseViewProps) {
  const [editing, setEditing] = useState(false);
  const [editContent, setEditContent] = useState(content);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState('');

  if (!content) {
    return (
      <Card>
        <CardContent className="py-8 text-center">
          <p className="text-sm text-muted-foreground">
            No knowledge file found. Create{' '}
            <code className="bg-muted px-1.5 py-0.5 rounded text-xs">
              orgs/{org}/knowledge.md
            </code>{' '}
            to get started.
          </p>
        </CardContent>
      </Card>
    );
  }

  async function handleSave() {
    setSaving(true);
    setSaveError('');
    try {
      const res = await fetch('/api/knowledge', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ org, content: editContent }),
      });
      if (!res.ok) {
        const data = await res.json();
        setSaveError(data.error || 'Failed to save');
      } else {
        setEditing(false);
        window.location.reload();
      }
    } catch (err) {
      setSaveError(String(err));
    }
    setSaving(false);
  }

  if (editing) {
    return (
      <Card>
        <CardHeader className="flex flex-row items-center justify-between pb-3">
          <CardTitle className="text-base">Edit Knowledge File</CardTitle>
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={() => { setEditing(false); setEditContent(content); }}>
              Cancel
            </Button>
            <Button size="sm" onClick={handleSave} disabled={saving}>
              {saving ? 'Saving...' : 'Save'}
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {saveError && <p className="text-sm text-destructive mb-3">{saveError}</p>}
          <textarea
            className="w-full h-[600px] p-4 rounded-md border bg-background font-mono text-sm resize-y focus:outline-none focus:ring-1 focus:ring-ring"
            value={editContent}
            onChange={(e) => setEditContent(e.target.value)}
          />
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-1">
      <div className="flex justify-between items-center mb-3">
        <p className="text-xs text-muted-foreground truncate max-w-[70%]">{filePath}</p>
        <Button variant="outline" size="sm" onClick={() => setEditing(true)}>
          Edit
        </Button>
      </div>
      <div className="rounded-lg border bg-card p-5 space-y-0.5">
        {renderMarkdown(content)}
      </div>
    </div>
  );
}
