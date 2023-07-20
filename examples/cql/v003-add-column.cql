ALTER TABLE winamp_cloud.recording_artist ADD (genres set<text>);
UPDATE winamp_cloud.recording_artist SET genres = {'Funk', 'Hip-hop', 'R&B', 'Soul'} WHERE name = 'Anderson Paak';
UPDATE winamp_cloud.recording_artist SET genres = {'Blues', 'Rock'} WHERE name = 'Eric Clapton';
UPDATE winamp_cloud.recording_artist SET genres = {'Power Pop', 'Stoner Doom'} WHERE name = 'King Tuff';
