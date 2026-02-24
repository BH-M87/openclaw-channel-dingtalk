import { randomUUID } from "node:crypto";
import { DWClient, TOPIC_ROBOT } from "dingtalk-stream";
import type { OpenClawConfig } from "openclaw/plugin-sdk";
import { buildChannelConfigSchema } from "openclaw/plugin-sdk";
import { getAccessToken } from "./auth";
import { createAICard, streamAICard, finishAICard } from "./card-service";
import { getConfig, isConfigured, resolveRelativePath, stripTargetPrefix } from "./config";
import { DingTalkConfigSchema } from "./config-schema.js";
import { ConnectionManager } from "./connection-manager";
import { isMessageProcessed, markMessageProcessed } from "./dedup";
import { handleDingTalkMessage } from "./inbound-handler";
import { getLogger } from "./logger-context";
import { dingtalkOnboardingAdapter } from "./onboarding.js";
import { resolveOriginalPeerId } from "./peer-id-registry";
import {
  detectMediaTypeFromExtension,
  sendMessage,
  sendProactiveMedia,
  sendBySession,
  uploadMedia,
} from "./send-service";
import type {
  DingTalkInboundMessage,
  GatewayStartContext,
  GatewayStopResult,
  ConnectionManagerConfig,
  DingTalkChannelPlugin,
  ResolvedAccount,
} from "./types";
import { ConnectionState } from "./types";
import { cleanupOrphanedTempFiles, getCurrentTimestamp } from "./utils";

const processingDedupKeys = new Set<string>();
const inboundCountersByAccount = new Map<
  string,
  {
    received: number;
    acked: number;
    dedupSkipped: number;
    inflightSkipped: number;
    processed: number;
    failed: number;
    noMessageId: number;
  }
>();
const INBOUND_COUNTER_LOG_EVERY = 10;

function getInboundCounters(accountId: string) {
  const existing = inboundCountersByAccount.get(accountId);
  if (existing) {
    return existing;
  }
  const created = {
    received: 0,
    acked: 0,
    dedupSkipped: 0,
    inflightSkipped: 0,
    processed: 0,
    failed: 0,
    noMessageId: 0,
  };
  inboundCountersByAccount.set(accountId, created);
  return created;
}

function logInboundCounters(log: any, accountId: string, reason: string): void {
  const stats = getInboundCounters(accountId);
  log?.info?.(
    `[${accountId}] Inbound counters (${reason}): received=${stats.received}, acked=${stats.acked}, processed=${stats.processed}, dedupSkipped=${stats.dedupSkipped}, inflightSkipped=${stats.inflightSkipped}, failed=${stats.failed}, noMessageId=${stats.noMessageId}`,
  );

  try {
    const streamResp = await axios.put(`${DINGTALK_API}/v1.0/card/streaming`, streamBody, {
      headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
    });
    log?.debug?.(
      `[DingTalk][AICard] Streaming response: status=${streamResp.status}, data=${JSON.stringify(streamResp.data)}`
    );

    // Update last updated time and state
    card.lastUpdated = Date.now();
    if (finished) {
      card.state = AICardStatus.FINISHED;
    } else if (card.state === AICardStatus.PROCESSING) {
      card.state = AICardStatus.INPUTING;
    }
  } catch (err: any) {
    // Handle 500 unknownError - likely cardTemplateKey mismatch with card template variables
    if (err.response?.status === 500 && err.response?.data?.code === 'unknownError') {
      const usedKey = streamBody.key;
      const cardTemplateId = card.config?.cardTemplateId || '(unknown)';
      const errorMsg =
        `⚠️ **[DingTalk] AI Card 串流更新失败 (500 unknownError)**\n\n` +
        `这通常是因为 \`cardTemplateKey\` (当前值: \`${usedKey}\`) 与钉钉卡片模板 \`${cardTemplateId}\` 中定义的正文变量名不匹配。\n\n` +
        `**建议操作**：\n` +
        `1. 前往钉钉开发者后台检查该模板的“变量管理”\n` +
        `2. 确保配置中的 \`cardTemplateKey\` 与模板中用于显示内容的字段变量名完全一致\n\n` +
        `*注意：当前及后续消息将自动转为 Markdown 发送，直到问题修复。*\n` +
        `*参考文档: https://github.com/soimy/openclaw-channel-dingtalk/blob/main/README.md#3-%E5%BB%BA%E7%AB%8B%E5%8D%A1%E7%89%87%E6%A8%A1%E6%9D%BF%E5%8F%AF%E9%80%89`;

      log?.error?.(
        `[DingTalk][AICard] Streaming failed with 500 unknownError. Key: ${usedKey}, Template: ${cardTemplateId}. ` +
          `Verify that "cardTemplateKey" matches the content field variable name in your card template.`
      );

      card.state = AICardStatus.FAILED;
      card.lastUpdated = Date.now();

      if (card.config) {
        // Send notification directly to user via Markdown fallback
        // We don't pass accountId here to ensure it doesn't try to use AI Card recursion
        try {
          await sendMessage(card.config, card.conversationId, errorMsg, { log });
        } catch (sendErr: any) {
          log?.warn?.(`[DingTalk][AICard] Failed to send error notification to user: ${sendErr.message}`);
        }
      }

      throw err;
    }

    // Handle 401 errors specifically - try to refresh token once
    if (err.response?.status === 401 && card.config) {
      log?.warn?.('[DingTalk][AICard] Received 401 error, attempting token refresh and retry...');
      try {
        card.accessToken = await getAccessToken(card.config, log);
        // Retry the streaming request with refreshed token
        const retryResp = await axios.put(`${DINGTALK_API}/v1.0/card/streaming`, streamBody, {
          headers: { 'x-acs-dingtalk-access-token': card.accessToken, 'Content-Type': 'application/json' },
        });
        log?.debug?.(`[DingTalk][AICard] Retry after token refresh succeeded: status=${retryResp.status}`);
        // Update state on successful retry
        card.lastUpdated = Date.now();
        if (finished) {
          card.state = AICardStatus.FINISHED;
        } else if (card.state === AICardStatus.PROCESSING) {
          card.state = AICardStatus.INPUTING;
        }
        return; // Success, exit function
      } catch (retryErr: any) {
        log?.error?.(`[DingTalk][AICard] Retry after token refresh failed: ${retryErr.message}`);
        // Fall through to mark as failed and throw
      }
    }

    // Ensure card state reflects the failure to prevent retry loops
    card.state = AICardStatus.FAILED;
    card.lastUpdated = Date.now();
    log?.error?.(
      `[DingTalk][AICard] Streaming update failed: ${err.message}, resp=${JSON.stringify(err.response?.data)}`
    );
    throw err;
  }
}

/**
 * Finalize AI Card: close streaming channel and update to FINISHED state
 * @param card AI Card instance
 * @param content Final content
 * @param log Logger instance
 */
async function finishAICard(card: AICardInstance, content: string, log?: Logger): Promise<void> {
  log?.debug?.(`[DingTalk][AICard] Starting finish, final content length=${content.length}`);

  // Send final content with isFull=true and isFinalize=true to close streaming
  // No separate state update needed - the streaming API handles everything
  await streamAICard(card, content, true, log);
}

// ============ End of New AI Card API Functions ============

// Send message with automatic mode selection (card/markdown)
// Card mode: if an active AI Card exists for the target, stream updates; otherwise fall back to markdown.
async function sendMessage(
  config: DingTalkConfig,
  conversationId: string,
  text: string,
  options: SendMessageOptions & { sessionWebhook?: string; accountId?: string } = {}
): Promise<{ ok: boolean; error?: string; data?: AxiosResponse }> {
  try {
    const messageType = config.messageType || 'markdown';
    const log = options.log || getLogger();

    if (messageType === 'card' && options.accountId) {
      const targetKey = `${options.accountId}:${conversationId}`;
      const activeCardId = activeCardsByTarget.get(targetKey);
      if (activeCardId) {
        const activeCard = aiCardInstances.get(activeCardId);
        if (activeCard && !isCardInTerminalState(activeCard.state)) {
          try {
            await streamAICard(activeCard, text, false, log);
            return { ok: true };
          } catch (err: any) {
            log?.warn?.(`[DingTalk] AI Card streaming failed, fallback to markdown: ${err.message}`);
            activeCard.state = AICardStatus.FAILED;
            activeCard.lastUpdated = Date.now();
          }
        } else {
          activeCardsByTarget.delete(targetKey);
        }
      }
    }

    // Fallback to markdown mode
    if (options.sessionWebhook) {
      await sendBySession(config, options.sessionWebhook, text, options);
      return { ok: true };
    }

    const result = await sendProactiveTextOrMarkdown(config, conversationId, text, options);
    return { ok: true, data: result };
  } catch (err: any) {
    options.log?.error?.(`[DingTalk] Send message failed: ${err.message}`);
    return { ok: false, error: err.message };
  }
}

// Message handler
async function handleDingTalkMessage(params: HandleDingTalkMessageParams): Promise<void> {
  const { cfg, accountId, data, sessionWebhook, log, dingtalkConfig } = params;
  const rt = getDingTalkRuntime();

  // Save logger reference globally for use by other methods
  currentLogger = log;

  log?.debug?.('[DingTalk] Full Inbound Data:', JSON.stringify(maskSensitiveData(data)));

  // 0. 清理过期的卡片缓存
  cleanupCardCache();

  // 1. 过滤机器人自身消息
  if (data.senderId === data.chatbotUserId || data.senderStaffId === data.chatbotUserId) {
    log?.debug?.('[DingTalk] Ignoring robot self-message');
    return;
  }

  const content = extractMessageContent(data);
  if (!content.text) return;

  const isDirect = data.conversationType === '1';
  const senderId = data.senderStaffId || data.senderId;
  const senderName = data.senderNick || 'Unknown';
  const groupId = data.conversationId;
  const groupName = data.conversationTitle || 'Group';

  // Register original peer IDs to preserve case-sensitive conversationId (base64)
  if (groupId) registerPeerId(groupId);
  if (senderId) registerPeerId(senderId);

  // 2. Check authorization for direct messages based on dmPolicy
  let commandAuthorized = true;
  if (isDirect) {
    const dmPolicy = dingtalkConfig.dmPolicy || 'open';
    const allowFrom = dingtalkConfig.allowFrom || [];

    if (dmPolicy === 'allowlist') {
      const normalizedAllowFrom = normalizeAllowFrom(allowFrom);
      const isAllowed = isSenderAllowed({ allow: normalizedAllowFrom, senderId });

      if (!isAllowed) {
        log?.debug?.(`[DingTalk] DM blocked: senderId=${senderId} not in allowlist (dmPolicy=allowlist)`);

        // Notify user with their sender ID so they can request access
        try {
          await sendBySession(
            dingtalkConfig,
            sessionWebhook,
            `⛔ 访问受限\n\n您的用户ID：\`${senderId}\`\n\n请联系管理员将此ID添加到允许列表中。`,
            { log }
          );
        } catch (err: any) {
          log?.debug?.(`[DingTalk] Failed to send access denied message: ${err.message}`);
        }

        return;
      }

      log?.debug?.(`[DingTalk] DM authorized: senderId=${senderId} in allowlist`);
    } else if (dmPolicy === 'pairing') {
      // Pairing mode: check if sender is in the allow-from store (paired users)
      // Read paired users from the persistent store
      const storeAllowFrom = await rt.channel.pairing.readAllowFromStore('dingtalk').catch(() => []);
      const effectiveAllowFrom = [...allowFrom.map(String), ...storeAllowFrom];
      const normalizedEffective = normalizeAllowFrom(effectiveAllowFrom);
      // Note: cannot use isSenderAllowed here because it returns true for empty lists.
      // For pairing mode, empty list means nobody is paired yet.
      const isPaired = normalizedEffective.hasEntries && (
        normalizedEffective.hasWildcard ||
        (senderId ? normalizedEffective.entriesLower.includes(senderId.toLowerCase()) : false)
      );

      if (!isPaired) {
        // User is not paired — create a pairing request
        const { code, created } = await rt.channel.pairing.upsertPairingRequest({
          channel: 'dingtalk',
          id: senderId,
          meta: { name: senderName },
        });

        if (created) {
          log?.info?.(`[DingTalk] Pairing request created: senderId=${senderId} name=${senderName}`);
        }

        // Send pairing reply to user
        try {
          const pairingReply = rt.channel.pairing.buildPairingReply({
            channel: 'dingtalk',
            idLine: `Your DingTalk User ID: ${senderId}`,
            code,
          });
          await sendBySession(dingtalkConfig, sessionWebhook, pairingReply, { log });
        } catch (err: any) {
          log?.debug?.(`[DingTalk] Failed to send pairing reply: ${err.message}`);
        }

        // Block the message — user must be paired first
        return;
      }

      log?.debug?.(`[DingTalk] DM authorized via pairing: senderId=${senderId}`);
      // Compute commandAuthorized for paired users
      const shouldCompute = rt.channel.commands.shouldComputeCommandAuthorized(content.text, cfg);
      if (shouldCompute) {
        commandAuthorized = rt.channel.commands.resolveCommandAuthorizedFromAuthorizers({
          useAccessGroups: cfg.commands?.useAccessGroups !== false,
          authorizers: [
            { configured: normalizedEffective.hasEntries, allowed: true },
          ],
        });
      }
    } else {
      // 'open' policy - allow all
      commandAuthorized = true;
    }
  } else {
    // 群组通道授权
    const groupPolicy = dingtalkConfig.groupPolicy || 'open';
    const allowFrom = dingtalkConfig.allowFrom || [];

    if (groupPolicy === 'allowlist') {
      const normalizedAllowFrom = normalizeAllowFrom(allowFrom);
      const isAllowed = isSenderGroupAllowed({ allow: normalizedAllowFrom, groupId });

      if (!isAllowed) {
        log?.debug?.(
          `[DingTalk] Group blocked: conversationId=${groupId} senderId=${senderId} not in allowlist (groupPolicy=allowlist)`
        );

        try {
          await sendBySession(
            dingtalkConfig,
            sessionWebhook,
            `⛔ 访问受限\n\n您的群聊ID：\`${groupId}\`\n\n请联系管理员将此ID添加到允许列表中。`,
            { log, atUserId: senderId }
          );
        } catch (err: any) {
          log?.debug?.(`[DingTalk] Failed to send group access denied message: ${err.message}`);
        }

        return;
      }

      log?.debug?.(`[DingTalk] Group authorized: conversationId=${groupId} senderId=${senderId} in allowlist`);
    }
  }

  const route = rt.channel.routing.resolveAgentRoute({
    cfg,
    channel: 'dingtalk',
    accountId,
    // agent权限和openclaw保持一致
    peer: { kind: isDirect ? 'direct' : 'group', id: isDirect ? senderId : groupId },
  });

  const storePath = rt.channel.session.resolveStorePath(cfg.session?.store, { agentId: route.agentId });
  const workspacePath = resolveAgentWorkspaceDir(cfg, route.agentId);

  // Download media to agent workspace (must be after route is resolved for correct workspace)
  let mediaPath: string | undefined;
  let mediaType: string | undefined;
  if (content.mediaPath && dingtalkConfig.robotCode) {
    const media = await downloadMedia(dingtalkConfig, content.mediaPath, workspacePath, log);
    if (media) {
      mediaPath = media.path;
      mediaType = media.mimeType;
    }
  }
  const envelopeOptions = rt.channel.reply.resolveEnvelopeFormatOptions(cfg);
  const previousTimestamp = rt.channel.session.readSessionUpdatedAt({ storePath, sessionKey: route.sessionKey });

  // Group-specific: resolve config, track members, format member list
  const groupConfig = !isDirect ? resolveGroupConfig(dingtalkConfig, groupId) : undefined;
  // GroupSystemPrompt is injected into the system prompt on every turn (unlike
  // group intro which only fires on the first turn). Embed DingTalk IDs here so
  // the AI always has access to conversationId.
  const groupSystemPrompt = !isDirect
    ? [`DingTalk group context: conversationId=${groupId}`, groupConfig?.systemPrompt?.trim()]
        .filter(Boolean)
        .join('\n')
    : undefined;

  if (!isDirect) {
    noteGroupMember(storePath, groupId, senderId, senderName);
  }
  const groupMembers = !isDirect ? formatGroupMembers(storePath, groupId) : undefined;

  const fromLabel = isDirect ? `${senderName} (${senderId})` : `${groupName} - ${senderName}`;
  const body = rt.channel.reply.formatInboundEnvelope({
    channel: 'DingTalk',
    from: fromLabel,
    timestamp: data.createAt,
    body: content.text,
    chatType: isDirect ? 'direct' : 'group',
    sender: { name: senderName, id: senderId },
    previousTimestamp,
    envelope: envelopeOptions,
  });

  const to = isDirect ? senderId : groupId;
  const ctx = rt.channel.reply.finalizeInboundContext({
    Body: body,
    RawBody: content.text,
    CommandBody: content.text,
    From: to,
    To: to,
    SessionKey: route.sessionKey,
    AccountId: accountId,
    ChatType: isDirect ? 'direct' : 'group',
    ConversationLabel: fromLabel,
    GroupSubject: isDirect ? undefined : groupName,
    SenderName: senderName,
    SenderId: senderId,
    Provider: 'dingtalk',
    Surface: 'dingtalk',
    MessageSid: data.msgId,
    Timestamp: data.createAt,
    MediaPath: mediaPath,
    MediaType: mediaType,
    MediaUrl: mediaPath,
    GroupMembers: groupMembers,
    GroupSystemPrompt: groupSystemPrompt,
    GroupChannel: isDirect ? undefined : route.sessionKey,
    CommandAuthorized: commandAuthorized,
    OriginatingChannel: 'dingtalk',
    OriginatingTo: to,
  });

  await rt.channel.session.recordInboundSession({
    storePath,
    sessionKey: ctx.SessionKey || route.sessionKey,
    ctx,
    updateLastRoute: { sessionKey: route.mainSessionKey, channel: 'dingtalk', to, accountId },
    onRecordError: (err: unknown) => {
      log?.error?.(`[DingTalk] Failed to record inbound session: ${String(err)}`);
    },
  });

  log?.info?.(`[DingTalk] Inbound: from=${senderName} text="${content.text.slice(0, 50)}..."`);

  // Determine if we are in card mode, if so, create or reuse card instance first
  const useCardMode = dingtalkConfig.messageType === 'card';
  let currentAICard: AICardInstance | undefined;
  let lastCardContent = '';

  if (useCardMode) {
    // Try to reuse an existing active AI card for this target, if available
    const targetKey = `${accountId}:${to}`;
    const existingCardId = activeCardsByTarget.get(targetKey);
    const existingCard = existingCardId ? aiCardInstances.get(existingCardId) : undefined;

    // Only reuse cards that are not in terminal states
    if (existingCard && !isCardInTerminalState(existingCard.state)) {
      currentAICard = existingCard;
      log?.debug?.('[DingTalk] Reusing existing active AI card for this conversation.');
    } else {
      // Create a new AI card
      try {
        const aiCard = await createAICard(dingtalkConfig, to, data, accountId, log);
        if (aiCard) {
          currentAICard = aiCard;
        } else {
          log?.warn?.('[DingTalk] Failed to create AI card (returned null), fallback to text/markdown.');
        }
      } catch (err: any) {
        log?.warn?.(`[DingTalk] Failed to create AI card: ${err.message}, fallback to text/markdown.`);
      }
    }
  }

  // Feedback: Thinking...
  if (dingtalkConfig.showThinking !== false) {
    try {
      const thinkingText = '🤔 思考中，请稍候...';
      // AI card already has thinking state visually, so we only send thinking message for non-card modes
      if (useCardMode && currentAICard) {
        log?.debug?.('[DingTalk] AI Card in thinking state, skipping thinking message send.');
      } else {
        lastCardContent = thinkingText;
        await sendMessage(dingtalkConfig, to, thinkingText, {
          sessionWebhook,
          atUserId: !isDirect ? senderId : null,
          log,
          accountId,
        });
      }
    } catch (err: any) {
      log?.debug?.(`[DingTalk] Thinking message failed: ${err.message}`);
    }
  }

  const { queuedFinal } = await rt.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
    ctx,
    cfg,
    dispatcherOptions: {
      responsePrefix: '',
      deliver: async (payload: any) => {
        try {
          const textToSend = payload.markdown || payload.text;
          if (!textToSend) return;

          lastCardContent = textToSend;
          await sendMessage(dingtalkConfig, to, textToSend, {
            sessionWebhook,
            atUserId: !isDirect ? senderId : null,
            log,
            accountId,
          });
        } catch (err: any) {
          log?.error?.(`[DingTalk] Reply failed: ${err.message}`);
          throw err;
        }
      },
    },
  });

  // Finalize AI card
  if (useCardMode && currentAICard) {
    try {
      // Helper function to check if a value is a non-empty string
      const isNonEmptyString = (value: any): boolean => typeof value === 'string' && value.trim().length > 0;

      // Validate that we have actual content before finalization
      const hasLastCardContent = isNonEmptyString(lastCardContent);
      const hasQueuedFinalString = isNonEmptyString(queuedFinal);

      if (hasLastCardContent || hasQueuedFinalString) {
        const finalContent =
          hasLastCardContent && typeof lastCardContent === 'string'
            ? lastCardContent
            : typeof queuedFinal === 'string'
              ? queuedFinal
              : '';
        await finishAICard(currentAICard, finalContent, log);
      } else {
        // No textual content was produced; skip finalization with empty content
        log?.debug?.('[DingTalk] Skipping AI Card finalization because no textual content was produced.');
        // Still mark the card as finished to allow cleanup
        currentAICard.state = AICardStatus.FINISHED;
        currentAICard.lastUpdated = Date.now();
      }
    } catch (err: any) {
      log?.debug?.(`[DingTalk] AI Card finalization failed: ${err.message}`);
      // Ensure the AI card transitions to a terminal error state
      try {
        if (currentAICard.state !== AICardStatus.FINISHED) {
          currentAICard.state = AICardStatus.FAILED;
          currentAICard.lastUpdated = Date.now();
        }
      } catch (stateErr: any) {
        // Log state update failure at debug level to aid production debugging
        log?.debug?.(`[DingTalk] Failed to update card state to FAILED: ${stateErr.message}`);
      }
    }
  }
  // Note: Media cleanup is handled by openclaw's media storage mechanism
  // Files saved via rt.channel.media.saveMediaBuffer are managed automatically
}

// DingTalk Channel Definition (assembly layer).
// Heavy logic is delegated to service modules for maintainability.
export const dingtalkPlugin: DingTalkChannelPlugin = {
  id: "dingtalk",
  meta: {
    id: "dingtalk",
    label: "DingTalk",
    selectionLabel: "DingTalk (钉钉)",
    docsPath: "/channels/dingtalk",
    blurb: "钉钉企业内部机器人，使用 Stream 模式，无需公网 IP。",
    aliases: ["dd", "ding"],
  },
  configSchema: buildChannelConfigSchema(DingTalkConfigSchema),
  onboarding: dingtalkOnboardingAdapter,
  capabilities: {
    chatTypes: ["direct", "group"] as Array<"direct" | "group">,
    reactions: false,
    threads: false,
    media: true,
    nativeCommands: false,
    blockStreaming: false,
  },
  pairing: {
    idLabel: 'DingTalk User ID',
    notifyApproval: async ({ cfg, id }) => {
      try {
        const config = getConfig(cfg);
        await sendMessage(config, id, '✅ OpenClaw access approved. Send a message to start chatting.', {});
      } catch (err: any) {
        // Best-effort notification — approval still succeeds even if message fails
      }
    },
  },
  reload: { configPrefixes: ['channels.dingtalk'] },
  config: {
    listAccountIds: (cfg: OpenClawConfig): string[] => {
      const config = getConfig(cfg);
      return config.accounts && Object.keys(config.accounts).length > 0
        ? Object.keys(config.accounts)
        : isConfigured(cfg)
          ? ["default"]
          : [];
    },
    resolveAccount: (cfg: OpenClawConfig, accountId?: string | null) => {
      const config = getConfig(cfg);
      const id = accountId || "default";
      const account = config.accounts?.[id];
      const resolvedConfig = account || config;
      const configured = Boolean(resolvedConfig.clientId && resolvedConfig.clientSecret);
      return {
        accountId: id,
        config: resolvedConfig,
        enabled: resolvedConfig.enabled !== false,
        configured,
        name: resolvedConfig.name || null,
      };
    },
    defaultAccountId: (): string => "default",
    isConfigured: (account: ResolvedAccount): boolean =>
      Boolean(account.config?.clientId && account.config?.clientSecret),
    describeAccount: (account: ResolvedAccount) => ({
      accountId: account.accountId,
      name: account.config?.name || "DingTalk",
      enabled: account.enabled,
      configured: Boolean(account.config?.clientId),
    }),
  },
  security: {
    resolveDmPolicy: ({ account }: any) => ({
      policy: account.config?.dmPolicy || "open",
      allowFrom: account.config?.allowFrom || [],
      policyPath: "channels.dingtalk.dmPolicy",
      allowFromPath: "channels.dingtalk.allowFrom",
      approveHint: "使用 /allow dingtalk:<userId> 批准用户",
      normalizeEntry: (raw: string) => raw.replace(/^(dingtalk|dd|ding):/i, ""),
    }),
  },
  groups: {
    resolveRequireMention: ({ cfg }: any): boolean => getConfig(cfg).groupPolicy !== "open",
    resolveGroupIntroHint: ({ groupId, groupChannel }: any): string | undefined => {
      const parts = [`conversationId=${groupId}`];
      if (groupChannel) {
        parts.push(`sessionKey=${groupChannel}`);
      }
      return `DingTalk IDs: ${parts.join(", ")}.`;
    },
  },
  messaging: {
    normalizeTarget: (raw: string) => (raw ? raw.replace(/^(dingtalk|dd|ding):/i, "") : undefined),
    targetResolver: {
      looksLikeId: (id: string): boolean => /^[\w+\-/=]+$/.test(id),
      hint: "<conversationId>",
    },
  },
  outbound: {
    deliveryMode: "direct" as const,
    resolveTarget: ({ to }: any) => {
      const trimmed = to?.trim();
      if (!trimmed) {
        return {
          ok: false as const,
          error: new Error("DingTalk message requires --to <conversationId>"),
        };
      }
      const { targetId } = stripTargetPrefix(trimmed);
      const resolved = resolveOriginalPeerId(targetId);
      return { ok: true as const, to: resolved };
    },
    sendText: async ({ cfg, to, text, accountId, log }: any) => {
      const config = getConfig(cfg, accountId);
      try {
        const result = await sendMessage(config, to, text, { log, accountId });
        getLogger()?.debug?.(`[DingTalk] sendText: "${text}" result: ${JSON.stringify(result)}`);
        if (result.ok) {
          const data = result.data as any;
          const messageId = String(data?.processQueryKey || data?.messageId || randomUUID());
          return {
            channel: "dingtalk",
            messageId,
            meta: result.data
              ? { data: result.data as unknown as Record<string, unknown> }
              : undefined,
          };
        }
        throw new Error(
          typeof result.error === "string" ? result.error : JSON.stringify(result.error),
        );
      } catch (err: any) {
        throw new Error(
          typeof err?.response?.data === "string"
            ? err.response.data
            : err?.message || "sendText failed",
          { cause: err },
        );
      }
    },
    sendMedia: async ({
      cfg,
      to,
      mediaPath,
      filePath,
      mediaUrl,
      mediaType: providedMediaType,
      accountId,
      log,
    }: any) => {
      const config = getConfig(cfg, accountId);
      if (!config.clientId) {
        throw new Error("DingTalk not configured");
      }

      // Support mediaPath/filePath/mediaUrl aliases for better CLI compatibility.
      const rawMediaPath = mediaPath || filePath || mediaUrl;

      getLogger()?.debug?.(
        `[DingTalk] sendMedia called: to=${to}, mediaPath=${mediaPath}, filePath=${filePath}, mediaUrl=${mediaUrl}, rawMediaPath=${rawMediaPath}`,
      );

      if (!rawMediaPath) {
        throw new Error(
          `mediaPath, filePath, or mediaUrl is required. Received: ${JSON.stringify({
            to,
            mediaPath,
            filePath,
            mediaUrl,
          })}`,
        );
      }

      const actualMediaPath = resolveRelativePath(rawMediaPath);

      getLogger()?.debug?.(
        `[DingTalk] sendMedia resolved path: rawMediaPath=${rawMediaPath}, actualMediaPath=${actualMediaPath}`,
      );

      try {
        const mediaType = providedMediaType || detectMediaTypeFromExtension(actualMediaPath);
        const result = await sendProactiveMedia(config, to, actualMediaPath, mediaType, {
          log,
          accountId,
        });
        getLogger()?.debug?.(
          `[DingTalk] sendMedia: ${mediaType} file=${actualMediaPath} result: ${JSON.stringify(result)}`,
        );

        if (result.ok) {
          const data = result.data;
          const messageId = String(
            result.messageId || data?.processQueryKey || data?.messageId || randomUUID(),
          );
          return {
            channel: "dingtalk",
            messageId,
            meta: result.data
              ? { data: result.data as unknown as Record<string, unknown> }
              : undefined,
          };
        }
        throw new Error(
          typeof result.error === "string" ? result.error : JSON.stringify(result.error),
        );
      } catch (err: any) {
        throw new Error(
          typeof err?.response?.data === "string"
            ? err.response.data
            : err?.message || "sendMedia failed",
          { cause: err },
        );
      }
    },
  },
  gateway: {
    startAccount: async (ctx: GatewayStartContext): Promise<GatewayStopResult> => {
      const { account, cfg, abortSignal } = ctx;
      const config = account.config;
      if (!config.clientId || !config.clientSecret) {
        throw new Error("DingTalk clientId and clientSecret are required");
      }

      ctx.log?.info?.(`[${account.accountId}] Initializing DingTalk Stream client...`);

      cleanupOrphanedTempFiles(ctx.log);

      const client = new DWClient({
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        debug: config.debug || false,
        keepAlive: true,
      });

      // Disable built-in reconnect so ConnectionManager owns all retry/backoff behavior.
      (client as any).config.autoReconnect = false;

      client.registerCallbackListener(TOPIC_ROBOT, async (res: any) => {
        const messageId = res.headers?.messageId;
        const stats = getInboundCounters(account.accountId);
        stats.received += 1;
        try {
          if (messageId) {
            client.socketCallBackResponse(messageId, { success: true });
            stats.acked += 1;
          }
          const data = JSON.parse(res.data) as DingTalkInboundMessage;

          // Message deduplication key is bot-scoped to avoid cross-account conflicts.
          const robotKey = config.robotCode || config.clientId || account.accountId;
          const msgId = data.msgId || messageId;
          const dedupKey = msgId ? `${robotKey}:${msgId}` : undefined;

          if (!dedupKey) {
            ctx.log?.warn?.(`[${account.accountId}] No message ID available for deduplication`);
            stats.noMessageId += 1;
            await handleDingTalkMessage({
              cfg,
              accountId: account.accountId,
              data,
              sessionWebhook: data.sessionWebhook,
              log: ctx.log,
              dingtalkConfig: config,
            });
            stats.processed += 1;
            if (stats.received % INBOUND_COUNTER_LOG_EVERY === 0) {
              logInboundCounters(ctx.log, account.accountId, "periodic");
            }
            return;
          }

          if (isMessageProcessed(dedupKey)) {
            ctx.log?.debug?.(`[${account.accountId}] Skipping duplicate message: ${dedupKey}`);
            stats.dedupSkipped += 1;
            logInboundCounters(ctx.log, account.accountId, "dedup-skipped");
            return;
          }

          if (processingDedupKeys.has(dedupKey)) {
            ctx.log?.debug?.(
              `[${account.accountId}] Skipping in-flight duplicate message: ${dedupKey}`,
            );
            stats.inflightSkipped += 1;
            logInboundCounters(ctx.log, account.accountId, "inflight-skipped");
            return;
          }

          processingDedupKeys.add(dedupKey);
          try {
            await handleDingTalkMessage({
              cfg,
              accountId: account.accountId,
              data,
              sessionWebhook: data.sessionWebhook,
              log: ctx.log,
              dingtalkConfig: config,
            });
            stats.processed += 1;
            markMessageProcessed(dedupKey);
            if (stats.received % INBOUND_COUNTER_LOG_EVERY === 0) {
              logInboundCounters(ctx.log, account.accountId, "periodic");
            }
          } finally {
            processingDedupKeys.delete(dedupKey);
          }
        } catch (error: any) {
          stats.failed += 1;
          logInboundCounters(ctx.log, account.accountId, "failed");
          ctx.log?.error?.(`[${account.accountId}] Error processing message: ${error.message}`);
        }
      });

      // Guard against duplicate stop paths (abort signal + explicit stop).
      let stopped = false;

      const connectionConfig: ConnectionManagerConfig = {
        maxAttempts: config.maxConnectionAttempts ?? 10,
        initialDelay: config.initialReconnectDelay ?? 1000,
        maxDelay: config.maxReconnectDelay ?? 60000,
        jitter: config.reconnectJitter ?? 0.3,
        onStateChange: (state: ConnectionState, error?: string) => {
          if (stopped) {
            return;
          }
          ctx.log?.debug?.(
            `[${account.accountId}] Connection state changed to: ${state}${error ? ` (${error})` : ""}`,
          );
          if (state === ConnectionState.CONNECTED) {
            ctx.setStatus({
              ...ctx.getStatus(),
              running: true,
              lastStartAt: getCurrentTimestamp(),
              lastError: null,
            });
          } else if (state === ConnectionState.FAILED || state === ConnectionState.DISCONNECTED) {
            ctx.setStatus({
              ...ctx.getStatus(),
              running: false,
              lastError: error || `Connection ${state.toLowerCase()}`,
            });
          }
        },
      };

      ctx.log?.debug?.(
        `[${account.accountId}] Connection config: maxAttempts=${connectionConfig.maxAttempts}, ` +
          `initialDelay=${connectionConfig.initialDelay}ms, maxDelay=${connectionConfig.maxDelay}ms, ` +
          `jitter=${connectionConfig.jitter}`,
      );

      const connectionManager = new ConnectionManager(
        client,
        account.accountId,
        connectionConfig,
        ctx.log,
      );

      // Register abort listener before connect() so startup can be cancelled safely.
      if (abortSignal) {
        if (abortSignal.aborted) {
          ctx.log?.warn?.(
            `[${account.accountId}] Abort signal already active, skipping connection`,
          );

          ctx.setStatus({
            ...ctx.getStatus(),
            running: false,
            lastStopAt: getCurrentTimestamp(),
            lastError: "Connection aborted before start",
          });

          throw new Error("Connection aborted before start");
        }

        abortSignal.addEventListener("abort", () => {
          if (stopped) {
            return;
          }
          stopped = true;
          ctx.log?.info?.(
            `[${account.accountId}] Abort signal received, stopping DingTalk Stream client...`,
          );
          connectionManager.stop();

          ctx.setStatus({
            ...ctx.getStatus(),
            running: false,
            lastStopAt: getCurrentTimestamp(),
          });
        });
      }

      try {
        await connectionManager.connect();

        if (!stopped && connectionManager.isConnected()) {
          ctx.setStatus({
            ...ctx.getStatus(),
            running: true,
            lastStartAt: getCurrentTimestamp(),
            lastError: null,
          });
          ctx.log?.info?.(`[${account.accountId}] DingTalk Stream client connected successfully`);

          await connectionManager.waitForStop();
        } else {
          ctx.log?.info?.(
            `[${account.accountId}] DingTalk Stream client connect() completed but channel is ` +
              `not running (stopped=${stopped}, connected=${connectionManager.isConnected()})`,
          );
        }
      } catch (err: any) {
        ctx.log?.error?.(`[${account.accountId}] Failed to establish connection: ${err.message}`);

        ctx.setStatus({
          ...ctx.getStatus(),
          running: false,
          lastError: err.message || "Connection failed",
        });
        throw err;
      }

      return {
        stop: () => {
          if (stopped) {
            return;
          }
          stopped = true;
          ctx.log?.info?.(`[${account.accountId}] Stopping DingTalk Stream client...`);
          connectionManager.stop();

          ctx.setStatus({
            ...ctx.getStatus(),
            running: false,
            lastStopAt: getCurrentTimestamp(),
          });

          ctx.log?.info?.(`[${account.accountId}] DingTalk Stream client stopped`);
        },
      };
    },
  },
  status: {
    defaultRuntime: {
      accountId: "default",
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    },
    collectStatusIssues: (accounts: any[]) => {
      return accounts.flatMap((account) => {
        if (!account.configured) {
          return [
            {
              channel: "dingtalk",
              accountId: account.accountId,
              kind: "config" as const,
              message: "Account not configured (missing clientId or clientSecret)",
            },
          ];
        }
        return [];
      });
    },
    buildChannelSummary: ({ snapshot }: any) => ({
      configured: snapshot?.configured ?? false,
      running: snapshot?.running ?? false,
      lastStartAt: snapshot?.lastStartAt ?? null,
      lastStopAt: snapshot?.lastStopAt ?? null,
      lastError: snapshot?.lastError ?? null,
    }),
    probeAccount: async ({ account, timeoutMs }: any) => {
      if (!account.configured || !account.config?.clientId || !account.config?.clientSecret) {
        return { ok: false, error: "Not configured" };
      }
      try {
        const controller = new AbortController();
        const timeoutId = timeoutMs ? setTimeout(() => controller.abort(), timeoutMs) : undefined;
        try {
          await getAccessToken(account.config);
          return { ok: true, details: { clientId: account.config.clientId } };
        } finally {
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
        }
      } catch (error: any) {
        return { ok: false, error: error.message };
      }
    },
    buildAccountSnapshot: ({ account, runtime, snapshot, probe }: any) => ({
      accountId: account.accountId,
      name: account.name,
      enabled: account.enabled,
      configured: account.configured,
      clientId: account.config?.clientId ?? null,
      running: runtime?.running ?? snapshot?.running ?? false,
      lastStartAt: runtime?.lastStartAt ?? snapshot?.lastStartAt ?? null,
      lastStopAt: runtime?.lastStopAt ?? snapshot?.lastStopAt ?? null,
      lastError: runtime?.lastError ?? snapshot?.lastError ?? null,
      probe,
    }),
  },
};

export {
  sendBySession,
  createAICard,
  streamAICard,
  finishAICard,
  sendMessage,
  uploadMedia,
  sendProactiveMedia,
  getAccessToken,
  getLogger,
};
export { detectMediaTypeFromExtension } from "./media-utils";
