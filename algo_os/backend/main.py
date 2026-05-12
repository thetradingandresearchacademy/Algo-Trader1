import asyncio
from .ai_supervisor import AISupervisor

from .market_data_engine import MarketDataEngine
from .alpha_option_strike_engine import AlphaOptionStrikeEngine
from .explosive_breakout_engine import ExplosiveBreakoutEngine
from .gamma_squeeze_engine import GammaSqueezeEngine
from .liquidity_trap_engine import LiquidityTrapEngine
from .volatility_expansion_engine import VolatilityExpansionEngine
from .index_alpha_engine import IndexAlphaEngine
from .stock_scoring_engine import StockScoringEngine
from .master_decision_engine import MasterDecisionEngine
from .execution_engine import ExecutionEngine
from .trade_journal_engine import TradeJournalEngine
from .report_engine import ReportEngine
from .risk_engine import RiskEngine
from .system_monitor_engine import SystemMonitorEngine
from .stock_vwap_retest_engine import StockVWAPRetestEngine
from .regime_engine import RegimeEngine


class AlgoOS:

    def __init__(self):

        print("Initializing Algo Trading OS")

        # Core market engines
        self.market_data = MarketDataEngine()
        self.alpha_option = AlphaOptionStrikeEngine()
        self.explosive_breakout = ExplosiveBreakoutEngine()
        self.gamma_squeeze = GammaSqueezeEngine()
        self.liquidity_trap = LiquidityTrapEngine()
        self.volatility = VolatilityExpansionEngine()
        self.alpha = IndexAlphaEngine()
        self.stock_scoring = StockScoringEngine()
        self.mde = MasterDecisionEngine()
        self.execution = ExecutionEngine()

        # New SaaS-grade engines
        self.vwap_retest = StockVWAPRetestEngine()
        self.regime = RegimeEngine()

        # Monitoring Engine
        self.monitor = SystemMonitorEngine()

        # Monitoring / safety / reporting
        self.journal = TradeJournalEngine()
        self.risk = RiskEngine()
        self.supervisor = AISupervisor()
        self.report = ReportEngine()

        # Scanner Listener Engine
        from .scanner_listener import ScannerListenerEngine
        self.scanner_listener = ScannerListenerEngine()

    # -----------------------------------------------------
    # Engine Supervisor
    # -----------------------------------------------------

    async def run_engine(self, name, engine):

        print(f"Starting engine: {name}")
        restart_count = 0

        while True:
            try:
                await engine.start()
                break  # Clean exit

            except Exception as e:
                restart_count += 1
                delay = min(5 * restart_count, 60)
                print(f"ENGINE CRASHED: {name} → {e} | Restarting in {delay}s (attempt #{restart_count})")
                await asyncio.sleep(delay)

    # -----------------------------------------------------
    # System Start
    # -----------------------------------------------------

    async def start(self):
        print("🚀 Starting Algo Trading OS")
        
        # Optimization: Pre-load scrip master once to share across all engines
        try:
            from services.cache_manager import CacheManager
            await asyncio.to_thread(CacheManager().get_scrip_master)
        except Exception as e:
            print(f"⚠️ Cache pre-load warning: {e}")

        # 1. CORE ENGINES (Immediate)
        core_tasks = [
            asyncio.create_task(self.run_engine("MarketDataEngine", self.market_data)),
            asyncio.create_task(self.run_engine("RegimeEngine", self.regime)),
            asyncio.create_task(self.run_engine("RiskEngine", self.risk)),
            asyncio.create_task(self.run_engine("SystemMonitorEngine", self.monitor)),
            asyncio.create_task(self.run_engine("TradeJournalEngine", self.journal)),
        ]

        # 2. SECONDARY ENGINES (Staggered minimally)
        async def start_secondary():
            await asyncio.sleep(0.1)
            secondary_tasks = [
                asyncio.create_task(self.run_engine("ExecutionEngine", self.execution)),
                asyncio.create_task(self.run_engine("MasterDecisionEngine", self.mde)),
                asyncio.create_task(self.run_engine("AlphaOptionStrikeEngine", self.alpha_option)),
                asyncio.create_task(self.run_engine("VolatilityExpansionEngine", self.volatility)),
                asyncio.create_task(self.run_engine("LiquidityTrapEngine", self.liquidity_trap)),
                asyncio.create_task(self.run_engine("ExplosiveBreakoutEngine", self.explosive_breakout)),
                asyncio.create_task(self.run_engine("GammaSqueezeEngine", self.gamma_squeeze)),
                asyncio.create_task(self.run_engine("StockVWAPRetestEngine", self.vwap_retest)),
            ]
            await asyncio.gather(*secondary_tasks, return_exceptions=True)

        # 3. HEAVY ENGINES (Staggered minimally)
        async def start_heavy():
            await asyncio.sleep(0.2)
            heavy_tasks = [
                asyncio.create_task(self.run_engine("IndexAlphaEngine", self.alpha)),
                asyncio.create_task(self.run_engine("StockScoringEngine", self.stock_scoring)),
                asyncio.create_task(self.run_engine("ScannerListenerEngine", self.scanner_listener)),
                asyncio.create_task(self.run_engine("ReportEngine", self.report)),
                asyncio.create_task(self.run_engine("AISupervisor", self.supervisor)),
            ]
            await asyncio.gather(*heavy_tasks, return_exceptions=True)

        await asyncio.gather(
            *core_tasks,
            start_secondary(),
            start_heavy(),
            return_exceptions=True
        )


# -----------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------

if __name__ == "__main__":

    system = AlgoOS()

    try:
        asyncio.run(system.start())

    except KeyboardInterrupt:
        print("\n🛑 Algo Trading OS stopped manually.")