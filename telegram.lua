dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local item_type = nil
local item_name = nil
local item_value = nil
local item_channel = nil
local item_post = nil

local selftext = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local queue_resources = true

local discovered_outlinks = {}
local discovered_items = {}
local discovered_channels = {}
local bad_items = {}
local ids = {}
local covered_posts = {}
local to_queue = {}
local allowed_resources = {}
local is_sub_post = false
local api_url = nil
local api_peer = nil
local api_top_msg_id = nil
local api_discussion_hash = nil

local retry_url = false

local current_js = {
  ["widget-frame.js"] = "60",
  ["tgwallpaper.min.js"] = "3",
  ["tgsticker.js"] = "29",
  ["telegram-web.js"] = "14",
  ["telegram-widget.js"] = "20",
  ["discussion-widget.js"] = "9"
}

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  for _, v in pairs(discovered_outlinks) do
    if v[url] then
      return true
    end
  end
  if downloaded[url] or addedtolist[url]
    or (discovered_outlinks[""] and discovered_outlinks[""][url]) then
    return true
  end
  return false
end

encode_params = function(d)
  local result = ""
  for k, v in pairs(d) do
    if result ~= "" then
      result = result .. "&"
    end
    result = result .. k .. "=" .. urlparse.escape(v)
  end
  return result
end

discover_item = function(target, item)
  if not item then
    return nil
  end
  local shard = ""
  if string.match(item, "^https?://[^/]*telegram%.org/dl%?")
    or string.match(item, "^https?://[^/]*telegram%-cdn%.org/")
    or string.match(item, "^https?://[^/]*telesco%.pe/") then
    shard = "telegram"
  end
  if not target[shard] then
    target[shard] = {}
  end
  if not target[shard][item] then
    target[shard][item] = true
  end
end

find_item = function(url)
  local value = nil
  local type_ = nil
  --[[if not string.match(url, "^https?://t%.me/")
    and not string.match(url, "^https?://www%.t%.me/")
    and not string.match(url, "^https?://telegram%.me/")
    and not string.match(url, "^https?://www%.telegram%.me/") then
    value = url
    type_ = 'url'
  end]]
  if not value then
    value = string.match(url, "^https?://t%.me/s/([^/%?&]+)$")
    type_ = 'channel'
  end
  if not value then
    value = string.match(url, "^https?://t%.me/([^/]+/[^/]+)%?embed=1$")
    type_ = 'post'
  end
  if value and not covered_posts[string.lower(value)] then
    item_type = type_
    ids = {}
    if --[[type_ == "url" or]] type_ == "channel" then
      item_value = value
      if type_ == "channel" then
        item_channel = value
        ids[value] = true
      end
    elseif type_ == "post" then
      item_value = string.gsub(value, "/", ":")
      item_channel, item_post = string.match(value, "^([^/]+)/(.+)$")
      ids[item_post] = true
    end
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      abortgrab = false
      queue_resources = true
      retry_url = false
      api_url = nil
      api_peer = nil
      api_top_msg_id = nil
      api_discussion_hash = nil
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if url == api_url and not parenturl then
    return true
  end

  if string.match(url, "%?q=")
    or string.match(url, "%?before=")
    or string.match(url, "%?after=")
    or string.match(url, "^https?://[^/]+/[^/]+/[0-9]+%?comment=[0-9]+$") then
    return false
  end

  for _, pattern in pairs({
    "^https?://[^/]+%.me/([^/%?&#]+)",
    "^https?://[^/]+%.me/s/([^/%?&#]+)"
  }) do
    local new_channel = string.match(url, pattern)
    if new_channel
      and new_channel ~= "s"
      and new_channel ~= "api"
      and new_channel ~= "css" then
      discover_item(discovered_channels, "channel:" .. new_channel)
    end
  end

  if string.match(url, "^https?://[^/]*telesco%.pe/")
    or string.match(url, "^https?://[^/]*telegram%-cdn%.org/") then
    if item_type == "url" then
      return true
    end
    if allowed_resources[url] then
      return true
    end
    if not queue_resources then
      return false
    end
    allowed_resources[url] = true
    return allowed(url, parenturl)
  end

  if not string.match(url, "^https?://t%.me/")
    and not string.match(url, "^https?://www%.t%.me/")
    and not string.match(url, "^https?://telegram%.me/")
    and not string.match(url, "^https?://www%.telegram%.me/") then
    local temp = ""
    for c in string.gmatch(url, "(.)") do
      local b = string.byte(c)
      if b < 32 or b > 126 then
        c = string.format("%%%02X", b)
      end
      temp = temp .. c
    end
    discover_item(discovered_outlinks, string.match(temp, "^([^%s]+)"))
    return false
  end

  if not string.match(url, "^https?://t%.me/")
    and not string.match(url, "^https?://[^/]*telegram%.me/") then
    return false
  end

  if item_type == "post" then
    local has_post_id = false
    for s in string.gmatch(url, "([0-9a-zA-Z_]+)") do
      if ids[s] then
        has_post_id = true
      end
    end
    if has_post_id then
      for r in string.gmatch(url, "([^/%?&]+)") do
        if item_channel == r then
          return true
        end
      end
    end
  end

  if item_type == "channel" then
    if string.match(url, "^https?://[^/]+/[^/]+/[0-9]+")
      or string.match(url, "^https?://[^/]+/s/[^/]+/[0-9]+")
      or string.match(url, "%?before=")
      or string.match(url, "%?after=")
      or string.match(url, "%?q=") then
      return false
    end
    for s in string.gmatch(url, "([^/%?&]+)") do
      if ids[s] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if item_type == "url" then
    return false
  end

  if not processed(url) and allowed(url, parent["url"])
    and string.match(url, "^https?://[^/]+%.me/") then
    addedtolist[url] = true
    return true
  end]]
  
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  if item_type == "url" then
    return urls
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and string.match(url_, "^https?://[^/%.]+%..+")
      and allowed(url_, origurl) then
      if string.match(url_, "%?before=") or string.match(url_, "%?after=") then
        table.insert(urls, {
          url=url_,
          headers={
            ["X-Requested-With"]="XMLHttpRequest",
            ["Accept"]="application/json, text/javascript, */*; q=0.01"
          }
        })
      else
        table.insert(urls, { url=url_ })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function queue_discussion(data_before)
    io.stdout:write("Requesting discussion data before " .. data_before .. ".\n")
    io.stdout:flush()
    table.insert(urls, {
      url=api_url,
      post_data=encode_params({
        peer=api_peer,
        top_msg_id=api_top_msg_id,
        discussion_hash=api_discussion_hash,
        before_id=data_before,
        method="loadComments"
      }),
      headers={
        ["X-Requested-With"]="XMLHttpRequest",
        ["Accept"]="application/json, text/javascript, */*; q=0.01"
      }
    })
  end

  queue_resources = true

  local domain, path = string.match(url, "^https?://([^/]+)(/.+)$")
  if (
    domain == "www.t.me"
    or domain == "t.me"
    or domain == "www.telegram.me"
    or domain == "telegram.me"
  ) and not string.match(url, "%?embed=1") then
    check("https://t.me" .. path)
    check("https://telegram.me" .. path)
  elseif string.match(domain, "telesco%.pe") then
    check(string.gsub(url, "telesco%.pe", "telegram%-cdn%.org"))
  elseif string.match(domain, "telegram%-cdn%.org") then
    check(string.gsub(url, "telegram%-cdn%.org", "telesco%.pe"))
  end

  for url, _ in pairs(to_queue) do
    io.stdout:write("Queuing extra URL " .. url .. ".\n")
    io.stdout:flush()
    check(url)
  end
  to_queue = {}

  if allowed(url) and status_code < 300
    and string.match(url, "^https?://[^/]+%.me/") then
    html = read_file(file)
    if string.match(url, "^https?://[^/]+/[^/]+/[0-9]+%?embed=1$") then
      --[[local html_new = string.gsub(html, '<div%s+class="tgme_widget_message_user">.-</div>', "") 
      if html == html_new then
        io.stdout:write("No profile image.\n")
        io.stdout:flush()
        abort_item()
        return {}
      end
      html = html_new]]
      if is_sub_post then
        io.stdout:write("Found sub post.\n")
        io.stdout:flush()
        is_sub_post = false
        return {}
      end
      discover_item(discovered_outlinks, string.match(html, '<i[^>]+class="tgme_widget_message_user_photo[^"]+"[^>]+>%s*<img%s+src="([^"]+)">%s*</i>'))
      local base = string.match(url, "^([^%?]+)")
      check(base .. "?embed=1&discussion=1")
      --check(base .. "?embed=1&discussion=1&comments_limit=5")
      check(base)
      check(base .. "?embed=1")
      check(base .. "?embed=1&mode=tme")
      if string.match(html, "%?single") then
        check(base .. "?single")
        --check(base .. "?single=1")
        check(base .. "?embed=1&single=1")
        check(base .. "?embed=1&mode=tme&single=1")
      end
      check(string.gsub(url, "^(https?://[^/]+/)([^%?]+)%?.*", "%1s/%2"))
      --check(string.gsub(url, "^(https?://[^/]+/)([^%?]-)/([0-9]+)%?.*", "%1s/%2?before=%3"))
      --check(string.gsub(url, "^(https?://[^/]+/)([^%?]-)/([0-9]+)%?.*", "%1s/%2?after=%3"))
      --check(string.gsub(url, "^(https?://[^/]+/)([^%?]-)/([0-9]+)%?.*", "%1share/url?url=%1%2/%3"))
    elseif --[[string.match(url, "^https?://[^/]+/[^/]+/[0-9]+")
      or]] string.match(url, "^https?://[^/]+/s/[^/]+/[0-9]+") then
      queue_resources = false
    end
    if string.match(url, "%?embed=1&discussion=1$") then
      check(url .. "&comments_limit=5")
    end
    if string.match(url, "^https?://[^/]+/s/[^/%?&]+$") then
      check(string.gsub(url, "^(https?://[^/]+/)s/([^/%?&]+)$", "%1%2"))
      local highest_id = -1
      local actual_channel = nil
      for channel, id in string.gmatch(html, 'data%-post="([^/]+)/([0-9]+)"') do
        if string.lower(channel) == string.lower(item_channel) then
          actual_channel = channel
          id = tonumber(id)
          if id > highest_id then
            highest_id = id
          end
        end
      end
      if actual_channel then
        if highest_id > -1 then
          for i=0,highest_id do
            discover_item(discovered_items, "post:" .. actual_channel .. ":" .. tostring(i))
          end
        end
      end
      local image_url = string.match(html, '<meta%s+property="og:image"%s+content="([^"]+)"')
      local twitter_url = string.match(html, '<meta%s+property="twitter:image"%s+content="([^"]+)"')
      if image_url ~= twitter_url then
        io.stdout:write("Profile images not equal for og:image and twitter:image.\n")
        io.stdout:flush()
        abort_item()
        return {}
      end
      if image_url then
        check(image_url)
      end
      for newurl in string.gmatch(html, '<i%s+class="tgme_page_photo_image[^"]+"[^>]+>%s*<img%s+src="([^"]+)"') do
        check(newurl)
      end
    end
    if item_type == "post" and string.match(url, "^https?://[^/]+/[^/]+/[0-9]+$") then
      discover_item(discovered_outlinks, string.match(html, '<meta%s+property="og:image"%s+content="([^"]+)">'))
    end
    if item_type == "post"
      and (
        string.match(url, "^https?://[^/]+/s/[^/%?&]+%?")
        or string.match(url, "^https?://[^/]+/s/[^/]+/[0-9]+")
      ) then
      queue_resources = false
    end
    if string.match(url, "[%?&]discussion=1") then
      local data = JSON:decode(string.match(html, "TWidgetAuth%.init%(({.-})%);"))
      api_url = data['api_url']
      local form_data = string.match(html, "(<form[^>]+>.-</form>)")
      local data_before = string.match(html, '<div%s+class="tme_messages_more%s+accent_bghover%s+js%-messages_more"%s+data%-before="([0-9]+)">')
      if data_before then
        api_peer = string.match(form_data, '<input[^>]+name="peer"%s+value="([^"]+)"%s*/>')
        api_top_msg_id = string.match(form_data, '<input[^>]+name="top_msg_id"%s+value="([^"]+)"%s*/>')
        api_discussion_hash = string.match(form_data, '<input[^>]+name="discussion_hash"%s+value="([^"]+)"%s*/>')
        queue_discussion(data_before)
      end
    end
    if url == api_url then
      local data = JSON:decode(html)
      html = string.gsub(data["comments_html"], "\\", "")
      local data_before = string.match(html, '<div%s+class="tme_messages_more%s+accent_bghover%s+js%-messages_more"%s+data%-before="([0-9]+)">')
      if data_before then
        queue_discussion(data_before)
      end
    end
    html = string.gsub(html, "</span>", "")
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  find_item(url["url"])

  if abortgrab then
    abort_item()
    return false
  end

  if string.match(url["url"], "^https?://[^/]*telesco%.pe/")
    or string.match(url["url"], "^https?://[^/]*telegram%-cdn%.org/") then
    if http_stat["statcode"] == 404 then
      return true
    elseif http_stat["statcode"] ~= 200 then
      abort_item()
      return false
    end
  end

  if http_stat["statcode"] ~= 200 and not string.match(url["url"], "%?single") then
    io.stdout:write("Status code not 200\n")
    io.stdout:flush()
    retry_url = true
    return false
  end

  if string.match(url["url"], "^https?://[^/]+%.me/") then
    local html = read_file(http_stat["local_file"])
    if string.match(url["url"], "%?before=")
      or string.match(url["url"], "%?after=") then
      html = JSON:decode(html)
    end
    if url["url"] == api_url then
      local data = JSON:decode(html)
      if data["comments_cnt"] < 5 or not data["ok"] then
        io.stdout:write("Did not receive \"ok\" from API server.\n")
        io.stdout:flush()
        abort_item()
        return false
      end
      html = string.gsub(data["comments_html"], "\\", "")
    end
    if string.match(url["url"], "%?embed=1$") and string.match(html, "%?single") then
      local found_ids = {}
      local current_id = tonumber(item_post)
      for channel, id in string.gmatch(html, "([^/]+)/([0-9]+)%?single[^a-zA-Z0-9]") do
        if string.lower(channel) == string.lower(item_channel) then
          found_ids[tonumber(id)] = true
        end
      end
      while found_ids[current_id] do
        current_id = current_id - 1
      end
      current_id = current_id + 1
      local min_id = current_id
      if min_id ~= tonumber(item_post) then
        is_sub_post = true
        return false
      end
      while found_ids[current_id] do
        current_id = current_id + 1
      end
      current_id = current_id - 1
      local max_id = current_id
      for id=min_id,max_id do
        id = tostring(id)
        ids[id] = true
        covered_posts[string.lower(item_channel) .. "/" .. id] = true
        to_queue["https://t.me/" .. item_channel .. "/" .. id .. "?embed=1"] = true
      end
    end
    for js_name, version in string.gmatch(html, "([^/]+%.js)%?([0-9]+)") do
      if current_js[js_name] ~= version then
        io.stdout:write("Script " .. js_name .. " with version " .. version .. " is not known.\n")
        io.stdout:flush()
        abort_item()
        return false
      end
    end
    --[[if string.match(url["url"], "%?embed=1&discussion=1") then
      if string.match(html, '"comments_cnt"')
        and not string.match(html, '<div%s+class="tme_no_messages_found">') then
        io.stdout:write("Found discussions comments. Not currently supported.\n")
        io.stdout:flush()
        abort_item()
        return false
      end
      return true
    end]]
    if not string.match(html, "telegram%-cdn%.org")
      and not string.match(html, "telesco%.pe") then
      io.stdout:write("Could not find CDNs on " .. url["url"] .. ".\n")
      io.stdout:flush()
      if http_stat["statcode"] == 302
        and string.match(url["url"], "%?single") then
        io.stdout:write("Valid 302 ?single page.\n")
        io.stdout:flush()
      elseif not (
        item_type == "post"
        and string.match(html, '<div%s+class="tgme_page%s+tgme_page_post">')
        and string.match(html, '<div%s+class="tgme_page_widget">')
      ) and not (
        string.match(url["url"], "^https?://[^/]+/s/")
        and string.match(html, '<div%s+class="tgme_channel_info_header_username">')
        and string.match(html, '<div%s+class="tgme_channel_info_header_title">')
        and string.match(html, '<div%s+class="tgme_channel_info_counters">')
      ) and not (
        item_type == "channel"
        and string.match(url["url"], "^https?://[^/]+/[^/%?]+$")
        and string.match(html, 'href="/s/[^"/%?]+">Preview%s+channel<')
      ) and not (
        string.match(url["url"], "/share/url%?url=")
        and string.match(html, '<div%s+class="tgme_page_desc_header">')
        and string.match(html, '<a%s+class="tgme_action_button_new shine"%s+href="tg://msg_url%?url=[^"]+">Share</a>')
      ) and not (
        string.match(url["url"], "%?embed=1&discussion=1")
        and (
          not string.match(html, '<div%s+class="tme_no_messages_found">')
          or string.match(html, '<div%s+class="tme_no_messages_found">Discussion%s+is not%s+available%s+at the%s+moment%.')
          or string.match(html, '<div%s+class="tme_no_messages_found">Please%s+open%s+Telegram%s+to%s+view%s+this%s+discussion%s+from')
          or string.match(html, '<h3%s+class="tgme_post_discussion_header">%s*<span%s+class="js%-header">Comments</span>%s+on%s+<a%s+href="https?://t%.me/[^/]+/[0-9]+">this%s+post</a>%s*</h3>')
          or string.match(html, '<div%s+class="tme_no_messages_found">Array%s+</div>')
        )
      ) and not (
        url["url"] == api_url
        and string.match(html, '<span%s+class="tgme_widget_message_author_name"%s+dir="auto">')
        and string.match(html, '<div%s+class="tgme_widget_message_text%s+js%-message_reply_text"%s+dir="auto">')
        and string.match(html, '<input%s+type="hidden"%s+name="reply_to_id"%s+value="[0-9]+">')
      ) then
        retry_url = true
        return false
      else
        io.stdout:write("Still valid page.\n")
        io.stdout:flush()
      end
    end
    if not string.match(url["url"], "[%?&]discussion=1")
      and url["url"] ~= api_url then
      if string.match(url["url"], "[%?&]embed=1") then
        if string.match(html, "tgme_widget_message_error")
          or not string.match(html, "tgme_widget_message_author") then
          io.stdout:write("Post does not exist.\n")
          io.stdout:flush()
          retry_url = true
          return false
        end
      elseif http_stat["statcode"] == 200 then
        local image_domain = string.match(html, '<meta%s+property="og:image"%s+content="([^"]*)"')
        if not image_domain or (
          image_domain ~= ""
          and not string.match(image_domain, "telegram%-cdn%.org/")
          and not string.match(image_domain, "telesco%.pe/")
          and not string.match(image_domain, "telegram%.org/img/")
        ) then
          io.stdout:write("Main image has bad domain.\n")
          io.stdout:flush()
          retry_url = true
          return false
        end
      end
    end
  end

  retry_url = false
  tries = 0

  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  find_item(url["url"])

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code == 200 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if retry_url or status_code == 0 then
    io.stdout:write("Server returned bad response. Sleeping.\n")
    io.stdout:flush()
    local maxtries = 10
    if (item_type == "post" and string.match(url["url"], "%?embed=1$"))
      or (item_type == "channel" and string.match(url["url"], "^https?://t%.me/s/([^/%?&]+)$")) then
      io.stdout:write("Bad response on first URL.\n")
      io.stdout:flush()
      maxtries = 0
    end
    tries = tries + 1
    if tries > maxtries then
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    os.execute("sleep " .. math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    ))
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key, shard)
    local tries = 0
    local maxtries = 10
    local parameters = ""
    if shard ~= "" then
      parameters = "?shard=" .. shard
    end
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key .. parameters,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        break
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == maxtries then
      kill_grab()
    end
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["telegram-x2kj4uadm0lrniv"] = discovered_items,
    --["telegram-iy46ve7bql0k79p"] = discovered_channels,
    ["telegram-channels-aqpadsraxi2b78y"] = discovered_channels,
    ["urls-h051713fi1agegy"] = discovered_outlinks
  }) do
    for shard, urls_data in pairs(data) do
      print('queuing for', string.match(key, "^(.+)%-"), "on shard", shard)
      local items = nil
      local count = 0
      for item, _ in pairs(urls_data) do
        print("found item", item)
        if items == nil then
          items = item
        else
          items = items .. "\0" .. item
        end
        count = count + 1
        if count == 100 then
          submit_backfeed(items, key, shard)
          items = nil
          count = 0
        end
      end
      if items ~= nil then
        submit_backfeed(items, key, shard)
      end
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

