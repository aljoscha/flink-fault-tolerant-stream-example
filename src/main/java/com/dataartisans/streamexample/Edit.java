/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.streamexample;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Edit {
	private String channel;
	private String raw;
	private long time;
	private String source;

	// these we extract from the "raw" field
	private String title;
	private String user;
	private String unparsedFlags;
	private int diffBytes;
	private String diffUrl;
	private String summary;
	private Flags flags;

	public boolean parse() {
		Pattern p = Pattern.compile("\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)");
		Matcher m = p.matcher(getRaw());



		if (m.find() && m.groupCount() == 6) {
			String title = m.group(1);
			String flags = m.group(2);
			String diffUrl = m.group(3);
			String user = m.group(4);
			int byteDiff = Integer.parseInt(m.group(5));
			String summary = m.group(6);

			Flags flagObject = new Flags(
					flags.contains("M"),
					flags.contains("N"),
					flags.contains("!"),
					flags.contains("B"),
					title.startsWith("Special:"),
					flags.startsWith("Talk:"));

			setTitle(title);
			setUser(user);
			setUnparsedFlags(flags);
			setDiffBytes(byteDiff);
			setDiffUrl(diffUrl);
			setSummary(summary);
			setFlags(flagObject);
			return true;
		} else {
			return false;
		}
	}

	public String getChannel() {
		return channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getRaw() {
		return raw;
	}

	public void setRaw(String raw) {
		this.raw = raw;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getUnparsedFlags() {
		return unparsedFlags;
	}

	public void setUnparsedFlags(String unparsedFlags) {
		this.unparsedFlags = unparsedFlags;
	}

	public int getDiffBytes() {
		return diffBytes;
	}

	public void setDiffBytes(int diffBytes) {
		this.diffBytes = diffBytes;
	}

	public String getDiffUrl() {
		return diffUrl;
	}

	public void setDiffUrl(String diffUrl) {
		this.diffUrl = diffUrl;
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public Flags getFlags() {
		return flags;
	}

	public void setFlags(Flags flags) {
		this.flags = flags;
	}

	@Override
	public String toString() {
		return "Edit{" +
				"channel='" + channel + '\'' +
				", raw='" + raw + '\'' +
				", time=" + time +
				", source='" + source + '\'' +
				", title='" + title + '\'' +
				", user='" + user + '\'' +
				", unparsedFlags='" + unparsedFlags + '\'' +
				", diffBytes=" + diffBytes +
				", diffUrl='" + diffUrl + '\'' +
				", summary='" + summary + '\'' +
				", flags=" + flags +
				'}';
	}

	public static class Flags {
		private boolean isMinor;
		private boolean isNew;
		private boolean isUnpatrolled;
		private boolean isBotEdit;
		private boolean isSpecial;
		private boolean isTalk;

		public Flags(
				boolean isMinor,
				boolean isNew,
				boolean isUnpatrolled,
				boolean isBotEdit,
				boolean isSpecial,
				boolean isTalk) {
			this.isMinor = isMinor;
			this.isNew = isNew;
			this.isUnpatrolled = isUnpatrolled;
			this.isBotEdit = isBotEdit;
			this.isSpecial = isSpecial;
			this.isTalk = isTalk;
		}

		public boolean isMinor() {
			return isMinor;
		}

		public boolean isNew() {
			return isNew;
		}

		public boolean isUnpatrolled() {
			return isUnpatrolled;
		}

		public boolean isBotEdit() {
			return isBotEdit;
		}

		public boolean isSpecial() {
			return isSpecial;
		}

		public boolean isTalk() {
			return isTalk;
		}

		@Override
		public String toString() {
			return "Flags{" +
					"isMinor=" + isMinor +
					", isNew=" + isNew +
					", isUnpatrolled=" + isUnpatrolled +
					", isBotEdit=" + isBotEdit +
					", isSpecial=" + isSpecial +
					", isTalk=" + isTalk +
					'}';
		}
	}
}
