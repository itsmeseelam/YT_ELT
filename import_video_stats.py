import video_stats

print(f'running script video_stats.py as a module')
print(f'Channel Playlist Id : {video_stats.get_playlist_id("https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle=")}')